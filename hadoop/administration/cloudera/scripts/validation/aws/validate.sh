./01_validate_hduser.sh ${1}
./02_validate_sudo_access.sh ${1}
./03_validate_iptables.sh ${1}
./04_validate_password_authentication.sh ${1}
./05_validate_selinux.sh ${1}
./06_validate_swappiness.sh ${1}
