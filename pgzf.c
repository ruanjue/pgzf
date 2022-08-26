#include "pgzf.h"

int usage(int ret){
	fprintf(stdout,
	"PGZF: Parallel blocked gzip file IO\n"
	"Author: Jue Ruan <ruanjue@caas.cn>\n"
	"Version: %s\n"
	"Usage: pgzf [options] file1 [file2 ...]\n"
	"Options:\n"
	" -t <int>    Number of threads, [8]\n"
	" -o <string> Output file name, support directory\n"
	" -f          Force to overwrite\n"
	" -a          Append instead of overwrite, not applicable for -o directory\n"
	" -x          Delete input files after compress\n"
	" -b <int>    Block size in MB, 1 ~ 256 [1]\n"
	"             '-b 1,8000' means 1 MB block size + 8000 blocks per group\n"
	"             that is one indexed group contains 8000 * 1MB bytes original data\n"
	" -l <int>    Compress level, 1-9, see gzip, [6]\n"
	" -d          Decompress mode\n"
	" -p <int>    Decompress at uncompressed position <-p>, [0]\n"
	"             '-p 1000' means seek to the 1000th byte\n"
	"             '-p -10' means seek to the 10th block\n"
	" -q <int>    Decompess only <-q> blocks, [0]\n"
	"             '-p -10 -q 1' means fetch the 10th block\n"
	" -v          Verbose\n"
	" -V          Print version information and exit\n"
	" -h          Show this document\n"
	"\n"
	"File format:\n"
	" PGZF fellows standard GZIP format (rfc1952), and is blocked compressed.\n"
	" Program pgzf can decompress .pgzf and .gz files. When decompressing .gz files,\n"
	" pgzf is in fact a buffered gzip reader. Also, .pgzf files can be decompressed\n"
	" by program gzip.\n"
	"\n"
	, TOSTR(VERSION));
	return ret;
}

int main(int argc, char **argv){
	PGZF *pz;
	char *outf, *ftag;
	FILE *in, *out;
	u1v *buffs;
	b8i seekoff, read_blks, limit_blks;
	u4i bufsize, nbyte, grp_blocks;
	int c, rw, ncpu, level, overwrite, append, del, is_dir, verbose;
	rw = PGZF_MODE_W;
	ncpu = 8;
	bufsize = PGZF_DEFAULT_BUFF_SIZE;
	grp_blocks = 8000;
	level = 6;
	overwrite = 0;
	append = 0;
	del = 0;
	verbose = 0;
	seekoff = 0;
	read_blks = 0;
	limit_blks = 0;
	outf = NULL;
	while((c = getopt(argc, argv, "hdxfat:b:l:o:p:q:vV")) != -1){
		switch(c){
			case 'h': return usage(0);
			case 't': ncpu = atoi(optarg); break;
			case 'b':
				do {
					char *ptr, *beg;
					beg = optarg;
					bufsize = (atol(beg) << 20);
					if((ptr = index(beg, ',')) == NULL) break;
					beg = ptr + 1;
					grp_blocks = atoi(beg);
				} while(0);
				break;
			case 'l': level = atoi(optarg); break;
			case 'f': overwrite = 1; break;
			case 'a': append = 1; break;
			case 'o': outf = optarg; break;
			case 'x': del = 1; break;
			case 'd': rw = PGZF_MODE_R; break;
			case 'p': seekoff = atol(optarg); break;
			case 'q': limit_blks = atol(optarg); break;
			case 'v': verbose = 1; break;
			case 'V': fprintf(stdout, "pgzf %s\n", TOSTR(VERSION)); return 0;
			default: return usage(1);
		}
	}
	if(optind == argc){
		return usage(1);
	}
	if(0 && del){
		if(outf == NULL && overwrite == 0){
			if(optind < argc){
				fprintf(stderr, " ** WARNNING: won't delete input files. To force delete input files, please specify -o or/and -f\n");
			}
			del = 0;
		}
	}
	is_dir = 0;
	out = NULL;
	if(outf){
		if(file_exists(outf)){
			if(append == 0 && overwrite == 0){
				fprintf(stderr, " ** ERROR: '%s' exists\n", outf);
				return 1;
			} else {
				for(c=optind;c<argc;c++){
					if(strcmp(outf, argv[c]) == 0){
						fprintf(stderr, " ** ERROR: The same file in INPUT and OUTPUT, '%s'\n", outf);
						return 1;
					}
				}
			}
			if(append){
				out = open_file_for_append(outf, NULL);
			} else {
				out = open_file_for_write(outf, NULL, overwrite);
			}
		} else if(dir_exists(outf)){
			is_dir = 1;
		} else {
			if(append){
				out = open_file_for_append(outf, NULL);
			} else {
				out = open_file_for_write(outf, NULL, overwrite);
			}
		}
	}
	buffs = init_u1v(bufsize);
	if(rw == PGZF_MODE_R){
		if(outf == NULL || is_dir){
			for(c=optind;c<argc;c++){
				if(strlen(argv[c]) < 4 || strcasecmp(argv[c] + strlen(argv[c]) - 3, ".gz")){
					fprintf(stderr, " ** ERROR: cannot auto generate output file name for '%s'\n", argv[c]);
					return 1;
				} else if(is_dir){
					char *rtag;
					rtag = relative_filename(argv[c]);
					rtag[strlen(rtag) - 3] = 0;
					ftag = malloc(strlen(outf) + 1 + strlen(rtag) + 1);
					sprintf(ftag, "%s/%s", outf, rtag);
					free(rtag);
					if(overwrite == 0 && file_exists(ftag)){
						fprintf(stderr, " ** ERROR: '%s' exists\n", ftag);
						return 1;
					}
					free(ftag);
				} else {
					ftag = strdup(argv[optind]);
					ftag[strlen(ftag) - 3] = 0;
					if(overwrite == 0 && file_exists(ftag)){
						fprintf(stderr, " ** ERROR: '%s' exists\n", ftag);
						return 1;
					}
					free(ftag);
				}
			}
		}
		do {
			in = open_file_for_read(argv[optind], NULL);
			if(outf == NULL){
				ftag = strdup(argv[optind]);
				ftag[strlen(ftag) - 3] = 0;
				out = open_file_for_write(ftag, NULL, overwrite);
				free(ftag);
			} else if(is_dir){
				char *rtag;
				rtag = relative_filename(argv[optind]);
				rtag[strlen(rtag) - 3] = 0;
				ftag = malloc(strlen(outf) + 1 + strlen(rtag) + 1);
				sprintf(ftag, "%s/%s", outf, rtag);
				free(rtag);
				out = open_file_for_write(ftag, NULL, overwrite);
				free(ftag);
			}
			pz = open_pgzf_reader(in, bufsize, ncpu);
			pz->verbose = verbose;
			if(seekoff > 0){
				seek_pgzf(pz, seekoff, SEEK_SET);
				seekoff = 0;
			} else if(seekoff < 0){
				seek_block_pgzf(pz, - seekoff);
				seekoff = 0;
			}
			if(limit_blks > 0){
				while(read_blks < limit_blks){
					if(read_block_pgzf(pz, buffs) == 0 && pz->eof) break;
					fwrite(buffs->buffer, 1, buffs->size, out);
					clear_u1v(buffs);
					read_blks ++;
				}
			} else {
				while((nbyte = read_pgzf(pz, buffs->buffer, buffs->cap))){
					fwrite(buffs->buffer, 1, nbyte, out);
				}
			}
			if(pz->error){
				fprintf(stderr, " ** ERROR: error code (%d)'\n", pz->error);
				return 1;
			}
			close_pgzf(pz);
			if(in != stdin){
				fclose(in);
#if 0
				if(del){
					unlink(argv[optind]);
				}
#endif
			}
			optind ++;
			if(outf == NULL || is_dir){
				fclose(out);
			}
		} while(optind < argc);
	} else {
		if(outf && !is_dir){
			pz = open_pgzf_writer(out, bufsize, ncpu, level);
			pz->verbose    = verbose;
			pz->grp_blocks = grp_blocks;
		} else {
			pz = NULL;
			for(c=optind;c<argc;c++){
				if(strlen(argv[c]) >= 4 && strcasecmp(argv[c] + strlen(argv[c]) - 3, ".gz") == 0){
					fprintf(stderr, " ** ERROR: file seems already compressed '%s'\n", argv[c]);
					return 1;
				} else if(strcmp(argv[c], "-") == 0){
					fprintf(stderr, " ** ERROR: Please specify output file when read from STDIN '%s'\n", argv[c]);
					return 1;
				} else if(is_dir){
					char *rtag;
					rtag = relative_filename(argv[c]);
					ftag = malloc(strlen(outf) + 1 + strlen(rtag) + 3 + 1);
					sprintf(ftag, "%s/%s.gz", outf, rtag);
					free(rtag);
					if(overwrite == 0 && file_exists(ftag)){
						fprintf(stderr, " ** ERROR: '%s' exists\n", ftag);
						return 1;
					}
					free(ftag);
				} else {
					ftag = malloc(strlen(argv[c]) + 4);
					sprintf(ftag, "%s.gz", argv[c]);
					if(overwrite == 0 && file_exists(ftag)){
						fprintf(stderr, " ** ERROR: '%s' exists\n", ftag);
						return 1;
					}
					free(ftag);
				}
			}
		}
		do {
			if(outf == NULL){
				ftag = malloc(strlen(argv[optind]) + 4);
				sprintf(ftag, "%s.gz", argv[optind]);
				out = open_file_for_write(ftag, NULL, overwrite);
				pz = open_pgzf_writer(out, bufsize, ncpu, level);
				pz->verbose    = verbose;
				pz->grp_blocks = grp_blocks;
				free(ftag);
			} else if(is_dir){
				char *rtag;
				rtag = relative_filename(argv[optind]);
				ftag = malloc(strlen(outf) + 1 + strlen(rtag) + 3 + 1);
				sprintf(ftag, "%s/%s.gz", outf, rtag);
				free(rtag);
				out = open_file_for_write(ftag, NULL, overwrite);
				pz = open_pgzf_writer(out, bufsize, ncpu, level);
				pz->verbose    = verbose;
				pz->grp_blocks = grp_blocks;
				free(ftag);
			}
			in = open_file_for_read(argv[optind], NULL);
			while((nbyte = fread(buffs->buffer, 1, buffs->cap, in))){
				write_pgzf(pz, buffs->buffer, nbyte);
			}
			if(in != stdin){
				fclose(in);
				if(del){
					unlink(argv[optind]);
				}
			}
			if(outf == NULL || is_dir){
				close_pgzf(pz);
				fclose(out);
			}
			optind ++;
		} while(optind < argc);
		if(outf && !is_dir){
			close_pgzf(pz);
		}
	}
	if(outf && !is_dir) fclose(out);
	free_u1v(buffs);
	return 0;
}
