## Getting Started
```sh
git clone https://github.com/ruanjue/pgzf
cd pgzf && make

# compress a.txt to a.txt.gz
./pgzf a.txt

# decompress a.txt.gz
./pgzf -d a.txt.gz
./pgzf -fd a.txt.gz

# random access a.txt.gz (50000-51000 bytes)
./pgzf -d a.txt.gz -p 50000 -q 1000

# compress a.txt in the manner of one block for each "//\n"
Not implemented in program, please call write_block_pgzf in your own code

# random access a.txt.gz (10th block)
./pgzf -d a.txt.gz -p -10 -q -1

```

## Introduction

PGZF fellows standard GZIP format (rfc1952), and is blocked compressed.
It maintains three tags: 1, `ZC` compressed block size; 2, `GC` size of a group of compressed blocks;
3, `IX` index information for a group. Default, a block takes 1 MB, a group contains 8000 blocks.
Each block contains `ZC` tag to fast fetch whole compressed block. The first block of a group contains
`GC` tag to tell program how many bytes for whole group. Program will locate the last block in one group 
to find the index data, there is no user data. PGZF compress/decompress in parallel of blocks.

## Citing pgzf

If you use pgzf, please cite:

> Parallel random access GZIP format file. Jue Ruan. https://github.com/ruanjue/pgzf


## Getting Help

Please use the [GitHub's Issues page][issue] if you have questions. You may
also directly contact Jue Ruan at ruanjue@gmail.com.
