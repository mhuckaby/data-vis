#!/usr/bin/env bash
#    Copyright 2018 Matthew Huckaby
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
cmd_curl="curl -o"
cmd_source="$cmd_curl ./downloads/bbs-list.html http://bbslist.textfiles.com/usbbs.html"
cmd_cache="cat ./downloads/bbs-list.html"
dir_data="downloads"
url_base="http://bbslist.textfiles.com/"

eval $cmd_source
eval $cmd_cache \
    | grep -o -E "([0-9]{1,3}\.txt)" \
    | sed 's/\.txt//' \
    | xargs -I@ echo "$cmd_curl $dir_data/@.txt $url_base@/@.txt" \
    | while read line; do $line; done
