#!/usr/bin/env python3
import os
import subprocess
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Tuple

class VMBackup:
    """
    Complete VM backup solution for KVM/libvirt environments with multi-disk support
    """

    def __init__(self):
        # Configuration
        self.BACKUP_ROOT = Path("/mnt/Backup")
        self.TEMP_STORAGE = Path("/storage")
        self.IMAGE_DIR = Path("/var/lib/libvirt/images")
        self.LOG_FILE = Path("/var/log/vm_backup.log")
        self.QGA_REQUIRED = True
        self.EXCLUDE_VMS = ["SPBAPP-EPT-CLONE", "TEMPLATE-*"]

        # State
        self.backup_status = 0
        self.running_vms = []
        
        # Setup
        self.setup_logging()

    def setup_logging(self):
        """Configure comprehensive logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%d.%m.%Y-%H:%M:%S',
            handlers=[
                logging.FileHandler(self.LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.log = logging.getLogger('VMBackup')

    def run_command(self, cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
        """Execute shell command with advanced error handling"""
        self.log.debug(f"Executing: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                check=check,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=300  # 5 minute timeout
            )
            return result
        except subprocess.TimeoutExpired:
            self.log.error(f"Command timed out: {' '.join(cmd)}")
            raise
        except subprocess.CalledProcessError as e:
            self.log.error(f"Command failed (code {e.returncode}): {' '.join(cmd)}")
            self.log.error(f"Error output: {e.stderr}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error executing command: {str(e)}")
            raise

    def validate_environment(self):
        """Validate all system requirements"""
        required_dirs = {
            self.IMAGE_DIR: "VM image directory",
            self.BACKUP_ROOT: "Backup destination",
            self.TEMP_STORAGE: "Temporary storage"
        }

        for path, desc in required_dirs.items():
            if not path.exists():
                raise RuntimeError(f"{desc} not found: {path}")
            if not os.access(path, os.W_OK):
                raise RuntimeError(f"No write permissions for {desc}: {path}")

        # Verify essential binaries
        required_binaries = ['virsh', 'qemu-img', 'du', 'df', 'cp']
        for binary in required_binaries:
            try:
                self.run_command(['which', binary])
            except subprocess.CalledProcessError:
                raise RuntimeError(f"Required binary not found: {binary}")

        if self.QGA_REQUIRED:
            try:
                self.run_command(['which', 'qemu-ga'])
            except subprocess.CalledProcessError:
                self.log.warning("qemu-guest-agent not found - --quiesce will be ignored")

    def get_running_vms(self) -> List[str]:
        """Get filtered list of running VMs"""
        result = self.run_command(['virsh', 'list', '--name', '--state-running'])
        vms = [vm.strip() for vm in result.stdout.splitlines() if vm.strip()]
        
        # Apply exclusion filters
        filtered_vms = []
        for vm in vms:
            if not any(excl in vm for excl in self.EXCLUDE_VMS):
                filtered_vms.append(vm)
        
        return filtered_vms

    def get_vm_disks(self, vm: str) -> Dict[str, dict]:
        """Get detailed disk information for VM"""
        disks = {}
        result = self.run_command(['virsh', 'domblklist', vm, '--details'])
        
        for line in result.stdout.splitlines()[2:]:  # Skip headers
            if not line.strip():
                continue
                
            parts = line.split()
            if len(parts) < 4:
                continue
                
            disk = {
                'type': parts[1],
                'device': parts[2],
                'source': parts[3],
                'target': parts[0]
            }
            disks[parts[0]] = disk
        
        return disks

    def calculate_required_space(self, disks: Dict[str, dict]) -> int:
        """Calculate total backup size in MB"""
        total_space = 0
        
        for disk_id, disk in disks.items():
            if disk['type'] != 'file' or not disk['source']:
                continue
                
            if not os.path.exists(disk['source']):
                self.log.warning(f"Disk source not found: {disk['source']}")
                continue
                
            try:
                du_result = self.run_command(['du', '-sm', disk['source']])
                total_space += int(du_result.stdout.split()[0])
            except Exception as e:
                self.log.error(f"Failed to calculate disk size: {str(e)}")
                raise
                
        return total_space

    def check_disk_space(self, vm: str, disks: Dict[str, dict]):
        """Verify sufficient space for backup"""
        required_space = self.calculate_required_space(disks)
        
        if required_space == 0:
            raise RuntimeError("No valid disks found for space calculation")
            
        df_result = self.run_command(['df', '-m', str(self.BACKUP_ROOT)])
        available_space = int(df_result.stdout.splitlines()[1].split()[3])
        
        buffer_multiplier = 2.5  # Extra space for snapshots and overhead
        required_with_buffer = int(required_space * buffer_multiplier)
        
        if available_space < required_with_buffer:
            raise RuntimeError(
                f"Insufficient space for {vm} backup. "
                f"Required: {required_with_buffer}M, Available: {available_space}M"
            )

    def create_directory(self, path: Path):
        """Create directory with proper permissions"""
        try:
            path.mkdir(parents=True, exist_ok=True)
            self.run_command(['chown', 'super:super', str(path)])
        except Exception as e:
            raise RuntimeError(f"Failed to create directory {path}: {str(e)}")

    def check_vm_state(self, vm: str):
        """Verify VM is running"""
        result = self.run_command(['virsh', 'domstate', vm])
        if result.stdout.strip() != 'running':
            raise RuntimeError(f"VM {vm} is not running")

    def check_qga(self, vm: str):
        """Check QEMU Guest Agent responsiveness"""
        if not self.QGA_REQUIRED:
            return
            
        try:
            self.run_command([
                'virsh', 'qemu-agent-command', 
                vm, '{"execute":"guest-ping"}'
            ])
        except Exception:
            raise RuntimeError("QEMU Guest Agent not responding")

    def create_snapshot(self, vm: str, disk_id: str, snap_path: Path):
        """Create disk snapshot with quiescing"""
        self.log.info(f"Creating snapshot for {vm} disk {disk_id}")
        
        try:
            cmd = [
                'virsh', 'snapshot-create-as',
                '--domain', vm,
                f"{vm}-{disk_id}-snapshot",
                '--disk-only',
                '--atomic',
                '--no-metadata',
                f"--diskspec", f"{disk_id},file={snap_path}"
            ]
            
            if self.QGA_REQUIRED:
                cmd.append('--quiesce')
                
            self.run_command(cmd)
        except Exception as e:
            raise RuntimeError(f"Failed to create snapshot for disk {disk_id}: {str(e)}")

    def backup_disk(self, source: str, dest: Path) -> bool:
        """Safely copy disk image with sparse handling"""
        try:
            self.run_command([
                'cp', '--sparse=always',
                source,
                str(dest)
            ])
            return True
        except Exception as e:
            self.log.error(f"Failed to backup disk {source}: {str(e)}")
            return False

    def commit_snapshot(self, vm: str, disk_id: str) -> bool:
        """Commit snapshot changes back to base image"""
        try:
            self.run_command([
                'virsh', 'blockcommit',
                vm, disk_id,
                '--active', '--verbose', '--pivot'
            ])
            return True
        except Exception:
            self.log.warning(f"Blockcommit failed for disk {disk_id}, attempting recovery...")
            
            # Recovery sequence
            try:
                self.run_command(['virsh', 'blockjob', vm, disk_id, '--abort'], check=False)
                self.run_command(['virsh', 'destroy', vm], check=False)
                self.run_command(['virsh', 'start', vm], check=False)
            except Exception as e:
                self.log.error(f"Recovery failed: {str(e)}")
                
            return False

    def backup_vm(self, vm: str) -> bool:
        """Complete backup procedure for a VM"""
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_dir = self.BACKUP_ROOT / vm / timestamp
        temp_dir = self.TEMP_STORAGE / f"{vm}-{timestamp}"
        
        self.log.info(f"Starting backup for VM: {vm}")
        
        try:
            # Get VM details
            disks = self.get_vm_disks(vm)
            if not disks:
                raise RuntimeError("No disks found for VM")
                
            self.check_disk_space(vm, disks)
            
            # Prepare directories
            self.create_directory(backup_dir)
            self.create_directory(temp_dir)
            
            # Save VM configuration
            config_file = backup_dir / f"{vm}.xml"
            self.run_command(['virsh', 'dumpxml', vm], stdout=open(config_file, 'w'))
            
            # Process each disk
            success_disks = 0
            for disk_id, disk in disks.items():
                if disk['type'] != 'file':
                    self.log.warning(f"Skipping non-file disk {disk_id} ({disk['type']})")
                    continue
                    
                disk_file = Path(disk['source'])
                if not disk_file.exists():
                    self.log.warning(f"Disk source not found: {disk_file}")
                    continue
                    
                # Create unique names for each disk
                disk_suffix = disk_id.replace('/', '-')
                snap_file = temp_dir / f"{vm}-{disk_suffix}.snapshot"
                backup_file = backup_dir / f"{vm}-{disk_suffix}.qcow2"
                
                try:
                    # Step 1: Create snapshot
                    self.create_snapshot(vm, disk_id, snap_file)
                    
                    # Step 2: Backup original disk
                    if not self.backup_disk(disk['source'], backup_file):
                        raise RuntimeError(f"Backup failed for disk {disk_id}")
                    
                    # Step 3: Commit changes
                    if not self.commit_snapshot(vm, disk_id):
                        raise RuntimeError(f"Commit failed for disk {disk_id}")
                        
                    success_disks += 1
                    
                except Exception as e:
                    self.log.error(f"Disk {disk_id} backup aborted: {str(e)}")
                    # Cleanup failed disk files
                    for f in [snap_file, backup_file]:
                        if f.exists():
                            f.unlink()
                    continue
            
            if success_disks == 0:
                raise RuntimeError("No disks were successfully backed up")
                
            # Final cleanup
            for item in temp_dir.glob('*'):
                item.replace(backup_dir / item.name)
            temp_dir.rmdir()
            
            self.log.info(f"Successfully backed up {success_disks}/{len(disks)} disks for {vm}")
            return True
            
        except Exception as e:
            self.log.error(f"VM backup failed: {str(e)}")
            
            # Emergency cleanup
            if temp_dir.exists():
                for item in temp_dir.glob('*'):
                    item.unlink(missing_ok=True)
                temp_dir.rmdir()
                
            if backup_dir.exists():
                for item in backup_dir.glob('*'):
                    item.unlink(missing_ok=True)
                backup_dir.rmdir()
                
            return False

    def main(self, selected_vms: Optional[List[str]] = None):
        """Main backup workflow"""
        try:
            self.log.info("=== Starting VM Backup Process ===")
            self.validate_environment()
            
            if not selected_vms:
                self.running_vms = self.get_running_vms()
                self.log.info(f"Discovered running VMs: {', '.join(self.running_vms)}")
                vms_to_backup = self.running_vms
            else:
                vms_to_backup = selected_vms
                self.log.info(f"Processing specified VMs: {', '.join(vms_to_backup)}")
            
            for vm in vms_to_backup:
                try:
                    self.log.info(f"Processing VM: {vm}")
                    self.check_vm_state(vm)
                    self.check_qga(vm)
                    
                    if not self.backup_vm(vm):
                        self.backup_status = 1
                        self.log.error(f"Backup failed for VM: {vm}")
                    else:
                        self.log.info(f"Successfully processed VM: {vm}")
                        
                except Exception as e:
                    self.log.error(f"Error processing VM {vm}: {str(e)}")
                    self.backup_status = 1
                    continue
            
            # Final status
            if self.backup_status == 0:
                self.log.info("=== All backups completed successfully ===")
            else:
                self.log.error("=== Backup completed with errors ===")
                
        except Exception as e:
            self.log.critical(f"Fatal error in backup process: {str(e)}")
            self.backup_status = 1
            
        sys.exit(self.backup_status)

if __name__ == "__main__":
    try:
        backup = VMBackup()
        
        # Handle command line arguments
        if len(sys.argv) > 1:
            backup.main(sys.argv[1:])
        else:
            backup.main()
            
    except KeyboardInterrupt:
        print("\nBackup interrupted by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Critical error: {str(e)}", file=sys.stderr)
        sys.exit(1)
