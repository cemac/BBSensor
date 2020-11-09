#! /bin/bash

str=$( curl -s https://raw.githubusercontent.com/first20hours/google-10000-english/master/20k.txt | perl -MList::Util=shuffle -e 'print shuffle<STDIN>' | sed -E 's/\b\(\w\)\{,4\}\b\s*//g' | head -n4 | tr '\n' ' ')
sudo echo "serverpi_access_key = $str" > /root/.serverpi
ssh-keygen -t rsa -f /root/.ssh/id_rsa -q -N $str
ssh-copy-id -i /root/.ssh/id_rsa serverpi@10.3.141.1
