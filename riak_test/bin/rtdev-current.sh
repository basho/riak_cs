cwd=$(pwd)
cd /tmp/rtcs
git reset HEAD --hard
git clean -fd
rm -rf /tmp/rtcs/current
mkdir /tmp/rtcs/current
cd $cwd
cp -p -P -R dev /tmp/rtcs/current
cd /tmp/rtcs
git add .
git commit -a -m "riak_test init" --amend
