#!/bin/bash
set -euo pipefail

cd ../../../toUpload/graphs/size_M
mkdir graph500-23       && cd graph500-23       && echo graph500-23       && wget -O graph500-23.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/graph500-23.tar.zst          && tar --use-compress-program=unzstd -xvf graph500-23.tar.zst       && cd ..
mkdir graph500-24       && cd graph500-24       && echo graph500-24       && wget -O graph500-24.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/graph500-24.tar.zst          && tar --use-compress-program=unzstd -xvf graph500-24.tar.zst       && cd ..
mkdir datagen-8_0-fb    && cd datagen-8_0-fb    && echo datagen-8_0-fb    && wget -O datagen-8_0-fb.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/datagen-8_0-fb.tar.zst    && tar --use-compress-program=unzstd -xvf datagen-8_0-fb.tar.zst    && cd ..
mkdir datagen-8_1-fb    && cd datagen-8_1-fb    && echo datagen-8_1-fb    && wget -O datagen-8_1-fb.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/datagen-8_1-fb.tar.zst    && tar --use-compress-program=unzstd -xvf datagen-8_1-fb.tar.zst    && cd ..
mkdir datagen-8_2-zf    && cd datagen-8_2-zf    && echo datagen-8_2-zf    && wget -O datagen-8_2-zf.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/datagen-8_2-zf.tar.zst    && tar --use-compress-program=unzstd -xvf datagen-8_2-zf.tar.zst    && cd ..
mkdir datagen-8_3-zf    && cd datagen-8_3-zf    && echo datagen-8_3-zf    && wget -O datagen-8_3-zf.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/datagen-8_3-zf.tar.zst    && tar --use-compress-program=unzstd -xvf datagen-8_3-zf.tar.zst    && cd ..
mkdir datagen-8_4-fb    && cd datagen-8_4-fb    && echo datagen-8_4-fb    && wget -O datagen-8_4-fb.tar.zst https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/graphalytics/datagen-8_4-fb.tar.zst    && tar --use-compress-program=unzstd -xvf datagen-8_4-fb.tar.zst    && cd ..
