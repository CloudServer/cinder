# Copyright (c) 2015 Parallels IP Holdings GmbH
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import errno
import json
import os
import re

from oslo_config import cfg

from cinder.brick.remotefs import remotefs
from cinder import exception
from cinder.i18n import _, _LI
from cinder.image import image_utils
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder.openstack.common import units
from cinder import utils
from cinder.volume.drivers import remotefs as remotefs_drv

VERSION = '1.0'

LOG = logging.getLogger(__name__)

vzstorage_opts = [
    cfg.StrOpt('vzstorage_shares_config',
               default='/etc/cinder/vzstorage_shares',
               help='File with the list of available vzstorage shares.'),
    cfg.BoolOpt('vzstorage_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space '
                      'rather than regular files when using raw format, '
                      'in which case volume creation takes lot of time.')),
    cfg.FloatOpt('vzstorage_used_ratio',
                 default=0.95,
                 help=('Percent of ACTUAL usage of the underlying volume '
                       'before no new volumes can be allocated to the volume '
                       'destination.')),
    cfg.StrOpt('vzstorage_default_volume_format',
               default='parallels',
               help=('Default format that will be used when creating volumes '
                     'if no volume format is specified. Can be set to: '
                     'raw, parallels, qcow2.')),
    cfg.StrOpt('vzstorage_mount_point_base',
               default='$state_path/mnt',
               help=('Base dir containing mount points for '
                     'vzstorage shares.')),
    cfg.ListOpt('vzstorage_mount_options',
                default=None,
                help=('Mount options passed to the vzstorage client. '
                      'See section of the pstorage-mount man page '
                      'for details.')),
]

CONF = cfg.CONF
CONF.register_opts(vzstorage_opts)


class PloopImgInfo(object):
    DELTA_REGEX = "Opening delta (.*)"
    SIZE_REGEX = "size:\s+(\d+)"
    BLOCKSIZE_REGEX = "blocksize:\s+(\d+)"
    SECTOR_SIZE = 512

    def __init__(self, image_path, execute):
        self._execute = execute
        info = self._get_info(image_path)

        self.image = info.get("image_file")
        self.backing_file = None
        self.file_format = "parallels"
        self.virtual_size = info.get("virtual_size")
        self.cluster_size = info.get("cluster_size")
        self.disk_size = info.get("disk_size")
        self.snapshots = []
        self.encrypted = False

    def _get_info(self, image_path):
        dd_path = os.path.join(image_path, "DiskDescriptor.xml")
        out, err = self._execute("ploop", "info", "-s", dd_path,
                                 run_as_root=True, check_exit_code=True)
        err_msg = "Invalid ploop output: %r" % out
        ret = {}

        m = re.search(self.DELTA_REGEX, out)
        if not m:
            raise exception.VzStorageException(err_msg)
        image_file = m.group(1)
        st = os.stat(image_file)
        ret["size"] = st.st_blocks * st.st_blksize
        ret["image_file"] = image_file

        m = re.search(self.SIZE_REGEX, out)
        if not m:
            raise exception.VzStorageException(err_msg)
        ret["virtual_size"] = int(m.group(1)) * self.SECTOR_SIZE

        m = re.search(self.BLOCKSIZE_REGEX, out)
        if not m:
            raise exception.VzStorageException(err_msg)
        ret["blocksize"] = int(m.group(1)) * self.SECTOR_SIZE

        return ret


class VZStorageDriver(remotefs_drv.RemoteFSSnapDriver):
    """Cinder driver for Virtuozzo Storage.

    Creates volumes as files on the mounted vzstorage cluster.

    Version history:
        1.0     - Initial driver.
    """
    driver_volume_type = 'vzstorage'
    driver_prefix = 'vzstorage'
    volume_backend_name = 'Virtuozzo_Storage'
    VERSION = VERSION
    SHARE_FORMAT_REGEX = r'(?:(\S+):\/)?([a-zA-Z0-9_-]+)(?::(\S+))?'

    def __init__(self, execute=putils.execute, *args, **kwargs):
        self._remotefsclient = None
        super(VZStorageDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(vzstorage_opts)
        self._execute_as_root = False
        root_helper = utils.get_root_helper()
        # base bound to instance is used in RemoteFsConnector.
        self.base = getattr(self.configuration,
                            'vzstorage_mount_point_base',
                            CONF.vzstorage_mount_point_base)
        opts = getattr(self.configuration,
                       'vzstorage_mount_options',
                       CONF.vzstorage_mount_options)

        self._remotefsclient = remotefs.RemoteFsClient(
            'vzstorage', root_helper, execute=execute,
            vzstorage_mount_point_base=self.base,
            vzstorage_mount_options=opts)

    def _qemu_img_info(self, path, volume_name):
        return super(VZStorageDriver, self)._qemu_img_info_base(
            path, volume_name, self.configuration.vzstorage_mount_point_base)

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: volume reference
        :param connector: connector reference
        """
        # Find active image
        active_file = self.get_active_image_from_info(volume)

        fmt = self._get_volume_format(volume)
        if fmt == "parallels":
            fmt = "ploop"

        data = {'export': volume['provider_location'],
                'format': fmt,
                'name': active_file,
                }

        return {
            'driver_volume_type': self.driver_volume_type,
            'data': data,
            'mount_point_base': self._get_mount_point_base(),
        }

    def do_setup(self, context):
        """Any initialization the volume driver does while starting."""
        super(VZStorageDriver, self).do_setup(context)

        config = self.configuration.vzstorage_shares_config
        if not os.path.exists(config):
            msg = (_("VzStorage config file at %(config)s doesn't exist.") %
                   {'config': config})
            LOG.error(msg)
            raise exception.VzStorageException(msg)

        if not os.path.isabs(self.base):
            msg = _("Invalid mount point base: %s.") % self.base
            LOG.error(msg)
            raise exception.VzStorageException(msg)

        used_ratio = self.configuration.vzstorage_used_ratio
        if not ((used_ratio > 0) and (used_ratio <= 1)):
            msg = _("VzStorage config 'vzstorage_used_ratio' invalid. "
                    "Must be > 0 and <= 1.0: %s.") % used_ratio
            LOG.error(msg)
            raise exception.VzStorageException(msg)

        self.shares = {}

        # Check if mount.fuse.pstorage is installed on this system;
        # note that we don't need to be root to see if the package
        # is installed.
        package = 'mount.fuse.pstorage'
        try:
            self._execute(package, check_exit_code=False,
                          run_as_root=False)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                msg = _('%s is not installed.') % package
                raise exception.VzStorageException(msg)
            else:
                raise

        self.configuration.nas_secure_file_operations = 'true'
        self.configuration.nas_secure_file_permissions = 'true'

    def _ensure_share_mounted(self, share):
        m = re.search(self.SHARE_FORMAT_REGEX, share)
        if not m:
            msg = (_("Invalid Virtuozzo Storage share specification: %r. "
                     "Must be: [MDS1[,MDS2],...:/]<CLUSTER NAME>[:PASSWORD].")
                   % share)
            raise exception.VzStorageException(msg)
        cluster_name = m.group(2)

        # set up logging to non-default path, so that it will
        # be possible to mount the same cluster to another mount
        # point by hand with default options.
        mnt_flags = ['-l', '/var/log/pstorage/%s-cinder.log.gz' % cluster_name]
        if self.shares.get(share) is not None:
            extra_flags = json.loads(self.shares[share])
            mnt_flags.extend(extra_flags)
        self._remotefsclient.mount(share, mnt_flags)

    def _find_share(self, volume_size_in_gib):
        """Choose VzStorage share among available ones for given volume size.

        For instances with more than one share that meets the criteria, the
        first suitable share will be selected.

        :param volume_size_in_gib: int size in GB
        """

        if not self._mounted_shares:
            raise exception.VzStorageNoSharesMounted()

        for share in self._mounted_shares:
            if self._is_share_eligible(share, volume_size_in_gib):
                break
        else:
            raise exception.VzStorageNoSuitableShareFound(
                volume_size=volume_size_in_gib)

        LOG.debug('Selected %s as target VzStorage share.', share)

        return share

    def _is_share_eligible(self, vz_share, volume_size_in_gib):
        """Verifies VzStorage share is eligible to host volume with given size.

        :param vz_share: vzstorage share
        :param volume_size_in_gib: int size in GB
        """

        used_ratio = self.configuration.vzstorage_used_ratio
        volume_size = volume_size_in_gib * units.Gi

        total_size, available, allocated = self._get_capacity_info(vz_share)

        if (allocated + volume_size) / total_size > used_ratio:
            LOG.debug('_is_share_eligible: %s is above '
                      'vzstorage_used_ratio.', vz_share)
            return False

        return True

    def _get_volume_format(self, volume):
        """Get volume format from volume object's metadata.
        Should be called for existing volumes only.
        """

        specs = volume.get('volume_metadata') or []
        for spec in specs:
            if spec.key == 'volume_format':
                return spec.value
        else:
            msg = "Volume format is not specified."
            raise exception.VzStorageException(msg)

    def _choose_volume_format(self, volume):
        """Chooses volume format for a new volume from (sorted by priority):

        1. 'volume_format' in volume's metadata
        2. 'volume_format' in volume type extra specs
        3. vzstorage_default_volume_format config option.
        """

        extra_specs = []

        metadata_specs = volume.get('volume_metadata') or []
        extra_specs += metadata_specs

        vol_type = volume.get('volume_type')
        if vol_type:
            volume_type_specs = vol_type.get('extra_specs') or []
            extra_specs += volume_type_specs

        for spec in extra_specs:
            if 'volume_format' in spec.key:
                return spec.value
        else:
                return self.configuration.vzstorage_default_volume_format

    def _ploop_file(self):
        return "root.hds"

    def _create_ploop_image(self, volume_path, volume_size):
        os.mkdir(volume_path)
        image_path = os.path.join(volume_path, self._ploop_file())
        self._execute("ploop", "init", "-s", "%dG" % volume_size, image_path)

    def _do_create_volume(self, volume):
        """Create a volume on given remote share.

        :param volume: volume reference
        """
        volume_format = self._choose_volume_format(volume)
        volume_path = self.local_path(volume)
        volume_size = volume['size']

        if os.path.exists(volume_path):
            msg = _('File already exists at %s.') % volume_path
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if volume_format == "parallels":
            self._create_ploop_image(volume_path, volume_size)
        elif volume_format == "raw":
            if getattr(self.configuration,
                       self.driver_prefix + '_sparsed_volumes'):
                self._create_sparsed_file(volume_path, volume_size)
            else:
                self._create_regular_file(volume_path, volume_size)
        else:
            msg = "Unsupported volume format: %s" % volume_format
            raise exception.VzStorageException(msg)

        self._set_rw_permissions_for_all(volume_path)
        return {"metadata": {"volume_format": volume_format}}

    def extend_volume(self, volume, size_gb):
        volume_path = self.local_path(volume)

        LOG.info(_LI('Extending volume %s.'), volume['id'])
        self._check_extend_volume_support(volume, size_gb)
        LOG.info(_LI('Resizing file to %sG...'), size_gb)

        volume_format = self._get_volume_format(volume)
        self._do_extend_volume(volume_path, size_gb, volume_format)

    def _do_extend_volume(self, volume_path, size_gb, volume_format):
        if volume_format == "parallels":
            self._execute('prl_disk_tool', 'resize', '--size',
                          '%dG' % size_gb, '--resize_partition',
                          '--hdd', volume_path,
                          run_as_root=True)
            volume_path = os.path.join(volume_path, self._ploop_file())
        else:
            image_utils.resize_image(volume_path, size_gb)

        if not self._is_file_size_equal(volume_path, size_gb):
            raise exception.ExtendVolumeError(
                reason='Resizing image file failed.')

    def _check_extend_volume_support(self, volume, size_gb):
        volume_path = self.local_path(volume)
        active_file = self.get_active_image_from_info(volume)
        active_file_path = os.path.join(self._local_volume_dir(volume),
                                        active_file)

        if active_file_path != volume_path:
            msg = _('Extend volume is only supported for this '
                    'driver when no snapshots exist.')
            raise exception.InvalidVolume(msg)

        extend_by = int(size_gb) - volume['size']
        if not self._is_share_eligible(volume['provider_location'],
                                       extend_by):
            raise exception.ExtendVolumeError(reason='Insufficient space to '
                                              'extend volume %s to %sG.'
                                              % (volume['id'], size_gb))

    def _is_file_size_equal(self, path, size):
        """Checks if file size at path is equal to size."""
        data = image_utils.qemu_img_info(path)
        virt_size = data.virtual_size / units.Gi
        return virt_size == size

    def _copy_volume_from_snapshot(self, snapshot, volume, volume_size):
        """Copy data from snapshot to destination volume.

        This is done with a qemu-img convert to raw/qcow2 from the snapshot
        qcow2.
        """

        LOG.debug("_copy_volume_from_snapshot: snapshot: %(snap)s, "
                  "volume: %(vol)s, volume_size: %(size)s.",
                  {'snap': snapshot['id'],
                   'vol': volume['id'],
                   'size': volume_size,
                   })

        info_path = self._local_path_volume_info(snapshot['volume'])
        snap_info = self._read_info_file(info_path)
        vol_dir = self._local_volume_dir(snapshot['volume'])
        out_format = "raw"

        forward_file = snap_info[snapshot['id']]
        forward_path = os.path.join(vol_dir, forward_file)

        # Find the file which backs this file, which represents the point
        # when this snapshot was created.
        img_info = self._qemu_img_info(forward_path,
                                       snapshot['volume']['name'])
        path_to_snap_img = os.path.join(vol_dir, img_info.backing_file)

        LOG.debug("_copy_volume_from_snapshot: will copy "
                  "from snapshot at %s.", path_to_snap_img)

        image_utils.convert_image(path_to_snap_img,
                                  self.local_path(volume),
                                  out_format)
        self._extend_volume(volume, volume_size)

    def _delete(self, path):
        # Note(lpetrut): this method is needed in order to provide
        # interoperability with Windows as it will be overridden.
        self._execute('rm', '-rf', path, run_as_root=True)

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        if not volume['provider_location']:
            msg = (_('Volume %s does not have provider_location '
                     'specified, skipping.') % volume['name'])
            LOG.error(msg)
            raise exception.VzStorageException(msg)

        self._ensure_share_mounted(volume['provider_location'])
        volume_dir = self._local_volume_dir(volume)
        mounted_path = os.path.join(volume_dir,
                                    self.get_active_image_from_info(volume))
        if os.path.exists(mounted_path):
            self._delete(mounted_path)
        else:
            LOG.info(_LI("Skipping deletion of volume %s "
                         "as it does not exist."), mounted_path)

        info_path = self._local_path_volume_info(volume)
        self._delete(info_path)

    def _recreate_ploop_desc(self, image_dir, image_file):
        desc_path = os.path.join(image_dir, "DiskDescriptor.xml")
        self._delete(desc_path)

        self._execute("ploop", "restore-descriptor", image_dir, image_file)

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""
        volume_format = self._get_volume_format(volume)
        image_path = self.local_path(volume)
        if volume_format == "parallels":
            image_path = os.path.join(image_path, self._ploop_file())

        image_utils.fetch_to_volume_format(
            context, image_service, image_id,
            image_path, volume_format,
            self.configuration.volume_dd_blocksize)

        if volume_format == "parallels":
            self._recreate_ploop_desc(self.local_path(volume), image_path)

        self._do_extend_volume(self.local_path(volume),
                               volume['size'],
                               volume_format)
