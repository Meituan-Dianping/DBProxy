#!/bin/bash

basedir=$(pwd)

set -e

while getopts "v:r:h" arg
do
        case $arg in
             v)
                echo "version_no: $OPTARG"
                version_no=$OPTARG
		;;
             r)
                echo "release_no: $OPTARG"
		release_no=$OPTARG
                ;;
	     h)
                echo "-v release_no -r release_no"
		exit 0
		;;
             ?)
            	echo "unkown argument"
        	exit 1
        	;;
        esac
done

if [[ ! "${version_no}" ]] || [[ ! "${release_no}" ]];then
	echo "error argument, plese exec with '-v version_no -r release_no'"
	exit 1
fi

rm -rf ~/rpmbuild/RPMS/x86_64/dbproxy-${version_no}-${release_no}.el6.x86_64.rpm
rm -rf ~/rpmbuild/RPMS/x86_64/dbproxy-debuginfo-${version_no}-${release_no}.el6.x86_64.rpm
rm -rf ~/rpmbuild/SRPMS/dbproxy-${version_no}-${release_no}.el6.src.rpm

rm -rf ~/rpmbuild/SOURCES/dbproxy-${version_no}
rm -rf ~/rpmbuild/SPECS/dbproxy-${version_no}.spec
mkdir ~/rpmbuild/SOURCES/dbproxy-${version_no}
cp -rf ../* ~/rpmbuild/SOURCES/dbproxy-${version_no}
cp -rf ./dbproxy.spec ~/rpmbuild/SPECS/dbproxy-${version_no}.spec

sed -i "s/Version:.*/Version: ${version_no}/"  ~/rpmbuild/SPECS/dbproxy-${version_no}.spec
sed -i "s/Release:.*/Release: ${release_no}%{?dist}/" ~/rpmbuild/SPECS/dbproxy-${version_no}.spec

cd ~/rpmbuild/SOURCES/
tar -zcf dbproxy-${version_no}.tar.gz dbproxy-${version_no}

cd ~
rpmbuild -ba ~/rpmbuild/SPECS/dbproxy-${version_no}.spec

cd $basedir
mv ~/rpmbuild/RPMS/x86_64/dbproxy-${version_no}-${release_no}.el6.x86_64.rpm ./
mv ~/rpmbuild/RPMS/x86_64/dbproxy-debuginfo-${version_no}-${release_no}.el6.x86_64.rpm ./
mv ~/rpmbuild/SRPMS/dbproxy-${version_no}-${release_no}.el6.src.rpm ./
