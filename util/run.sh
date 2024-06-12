CUR='/home/cm/dev/repo/RocksDB/plugin/flexfs/util/'
sudo rm -r /home/cm/tmp/*
/home/cm/dev/fdp/util/trim.sh nvme0n1
sudo $CUR/flexfs mkfs --fdp_bd=nvme0n1 --aux_path=/home/cm/tmp/ --force
/home/cm/dev/fdp/util/trim.sh nvme2n2
sudo $CUR/flexfs mkfs --zbd=nvme2n2 --aux_path=/home/cm/tmp/ --force
#sudo gdb --args ./flexfs test --fdp_bd=nvme1n1 --aux_path=/home/cm/tmp/
#sudo gdb --args ./flexfs mkfs --fdp_bd=nvme0n1 --aux_path=/home/cm/tmp/  --force
#sudo ./flexfs dump --fdp_bd=nvme0n1 --aux_path=/home/cm/tmp/
#sudo ./flexfs df --fdp_bd=nvme1n1 --aux_path=/home/cm/tmp/
#sudo gdb --args ./flexfs mkfs --zbd=nvme2n2 --aux_path=/home/cm/tmp/ --force
#sudo gdb --args ./flexfs dump --zbd=nvme2n2 --aux_path=/home/cm/tmp/

#sudo gdb --args ./flexfs mkfs --fdp_bd=nvme1n1 --aux_path=/home/cm/tmp/  --force
#sudo ./flexfs df --zbd=nvme2n2 --aux_path=/home/cm/tmp/
#sudo ./flexfs test --fdp_bd=nvme1n1 --aux_path=/home/cm/tmp/
#sudo gdb --args ./flexfs mkfs --fdp_bd=nvme1n1 --aux_path=/home/cm/tmp/  --force
