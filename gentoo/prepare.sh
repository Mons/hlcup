#!/bin/bash

[[ -e portage-latest.tar.bz2 ]] || wget http://distfiles.gentoo.org/snapshots/portage-latest.tar.bz2
[[ -e stableperl-5.22.0-1.001.tar.gz ]] || wget http://stableperl.schmorp.de/dist/stableperl-5.22.0-1.001.tar.gz
cd ../libs/Local-HLCup/ && perl Makefile.PL && rm *.tar.gz && make dist && cp Local-HLCup-0.01.tar.gz ../../gentoo/ ; cd -
