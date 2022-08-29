#ifndef __PGZF_RJ_H
#define __PGZF_RJ_H

#include <zlib.h>
#include "mem_share.h"
#include "list.h"
#include "sort.h"
#include "thread.h"

#define PGZF_DEFAULT_BUFF_SIZE	(1U << 20) // 1MB
#define PGZF_MAX_BUFF_SIZE	(1LLU << 32)
#define PGZF_BEG_HEAD_SIZE	32
#define PGZF_BEG_HEAD_GC_OFFSET	24
#define PGZF_DAT_HEAD_SIZE	20
#define PGZF_IDX_HEAD_SIZE	24
#define PGZF_HEAD_ZC_OFFSET	16
#define PGZF_TAIL_SIZE	8

#define PGZF_MODE_W	1 // pgzf writer
#define PGZF_MODE_R	2 // pgzf reader
#define PGZF_MODE_R_GZ	3 // gz reader
#define PGZF_MODE_R_UNKNOWN	4 // unknown file type

#define PGZF_FILETYPE_UNKNOWN	0
#define PGZF_FILETYPE_GZ	1
#define PGZF_FILETYPE_PGZF	2

#define PGZF_BLOCKTYPE_NIL	0
#define PGZF_BLOCKTYPE_BEG	1
#define PGZF_BLOCKTYPE_DAT	2
#define PGZF_BLOCKTYPE_IDX	3

struct PGZF;

#define PGZF_TASK_NULL	0
#define PGZF_TASK_DEFLATE	1
#define PGZF_TASK_INFLATE	2

thread_beg_def(pgz);
struct PGZF *pz;
u4i zcval, ixoff, soff;
u8i doff, gcval, fileoff;
u2i ixval;
u1v *dst, *src;
u4i token;
int level;
int task, eof, has, blocktype;
thread_end_def(pgz);

typedef struct PGZF {
	u4i ncpu, ridx, widx;
	int rw_mode, seekable, ftype;
	u4i bufsize; // MUST be multiple of 1MB
	u4i grp_blocks; // 8000
	u8v *blocks[2], *sels;
	u8i tot_in, tot_out, lst_in, lst_out, lst_off;
	u1v **dsts, **srcs, *tmp;
	z_stream *z;
	u8i offset;
	FILE *file;
	thread_def_shared_vars(pgz);
	int grp_status;
	int step; // used in decompress gzip file
	int error;
	int verbose;
} PGZF;

static inline void _num2bytes_pgzf(u1i *bs, u1i bl, u8i val){
	u1i i;
	for(i=0;i<bl;i++){
		bs[i] = (u1i)val;
		val >>= 8;
	}
}

static inline u8i _bytes2num_pgzf(u1i *bs, u1i bl){
	u8i val;
	u1i i;
	val = 0;
	for(i=0;i<bl;i++){
		val = val | (((u8i)bs[i]) << (8 * i));
	}
	return val;
}

/**
 * Please see https://tools.ietf.org/html/rfc1952
 */


static inline u4i _gen_beg_pgzf_header(u1i bs[32], u4i zsize, u8i gzsize){
	bs[0] = 0x1f; // GZIP ID1
	bs[1] = 0x8b; // GZIP ID2
	bs[2] = 8; // CM = deflate
	bs[3] = 0x4; // FLG = 0b00000100, FEXTRA is ture
	bs[4] = 0; // MTIME
	bs[5] = 0; // MTIME
	bs[6] = 0; // MTIME
	bs[7] = 0; // MTIME
	bs[8] = 0b10101010; // XFL, indicating pgzf
	bs[9] = 3; // OS = unix
	_num2bytes_pgzf(bs + 10, 2, 4 + 4 + 4 + 8); // XLEN
	bs[12] = 'Z'; // TAG ZC: compressed block size
	bs[13] = 'C';
	_num2bytes_pgzf(bs + 14, 2, 4); // TAG LEN
	_num2bytes_pgzf(bs + 16, 4, zsize);
	// gzsize will be updated after group end
	bs[20] = 'G'; // TAG GC: group compressed block size
	bs[21] = 'C';
	_num2bytes_pgzf(bs + 22, 2, 8); // TAG LEN
	_num2bytes_pgzf(bs + 24, 8, gzsize);
	return 32;
}

static inline u4i _gen_dat_pgzf_header(u1i bs[32], u4i zsize){
	bs[0] = 0x1f; // GZIP ID1
	bs[1] = 0x8b; // GZIP ID2
	bs[2] = 8; // CM = deflate
	bs[3] = 0x4; // FLG = 0b00000100, FEXTRA is ture
	bs[4] = 0; // MTIME
	bs[5] = 0; // MTIME
	bs[6] = 0; // MTIME
	bs[7] = 0; // MTIME
	bs[8] = 0b10101010; // XFL, indicating pgzf
	bs[9] = 3; // OS = unix
	_num2bytes_pgzf(bs + 10, 2, 8); // XLEN
	bs[12] = 'Z'; // TAG ZC: compressed block size
	bs[13] = 'C';
	_num2bytes_pgzf(bs + 14, 2, 4); // TAG LEN
	_num2bytes_pgzf(bs + 16, 4, zsize);
	return 20;
}

static inline u4i _gen_idx_pgzf_header(u1i bs[32], u4i zsize, u2i xcnt){
	bs[0] = 0x1f; // GZIP ID1
	bs[1] = 0x8b; // GZIP ID2
	bs[2] = 8; // CM = deflate
	bs[3] = 0x4; // FLG = 0b00000100, FEXTRA is ture
	bs[4] = 0; // MTIME
	bs[5] = 0; // MTIME
	bs[6] = 0; // MTIME
	bs[7] = 0; // MTIME
	bs[8] = 0b10101010; // XFL, indicating pgzf
	bs[9] = 3; // OS = unix
	_num2bytes_pgzf(bs + 10, 2, 8 + 4 + xcnt); // XLEN
	bs[12] = 'Z'; // TAG ZC: compressed block size
	bs[13] = 'C';
	_num2bytes_pgzf(bs + 14, 2, 4); // TAG LEN
	_num2bytes_pgzf(bs + 16, 4, zsize);
	bs[20] = 'I'; // TAG IX: nbin * {bzsize:u4i, busize:u4i}
	bs[21] = 'X';
	_num2bytes_pgzf(bs + 22, 2, xcnt); // TAG LEN
	return 24;
}

static inline void _gen_pgzf_tailer(u1i bs[8], u4i crc, u4i u_size){
	_num2bytes_pgzf(bs + 0, 4, crc);
	_num2bytes_pgzf(bs + 4, 4, u_size);
}

static inline u4i _zlib_raw_deflate_all(u1i *dst, u4i dlen, u1i *src, u4i slen, int level){
	z_stream Z, *z;
	u4i ret;
	z = &Z;
	ZEROS(z);
	deflateInit2(z, level, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY);
	z->avail_in = slen;
	z->next_in  = src;
	z->avail_out = dlen;
	z->next_out  = dst;
	deflate(z, Z_FINISH);
	ret = dlen - z->avail_out;
	deflateEnd(z);
	return ret;
}

static inline int _read_pgzf_header(FILE *in, u1v *src, u4i *hoff, u4i *zcval, u8i *gcval, u2i *ixval, u4i *ixoff){
	u4i off, val, sl, end;
	int ch, is_pgzf, xflag;
	char si1, si2;
	is_pgzf = 0;
	off = *hoff;
	if(src->size < off + 10){
		encap_u1v(src, 10);
		src->size += fread(src->buffer + src->size, 1, off + 10 - src->size, in);
	}
	if(zcval) zcval[0] = 0;
	if(gcval) gcval[0] = 0;
	if(ixval) ixval[0] = 0;
	if(ixoff) ixoff[0] = 0;
	// At least give 10 bytes
	if(src->size < off + 10) return PGZF_FILETYPE_UNKNOWN;
	if(src->buffer[off + 0] != 0x1f) return PGZF_FILETYPE_UNKNOWN;
	if(src->buffer[off + 1] != 0x8b) return PGZF_FILETYPE_UNKNOWN;
	if((src->buffer[off + 2] != 0x08) || (src->buffer[off + 2] & 0xE0)) return PGZF_FILETYPE_UNKNOWN;
	if(src->buffer[off + 8] == 0b10101010) is_pgzf = 1;
	xflag = src->buffer[off + 3];
	off += 10;
	if(xflag & 0x04){ // FEXTRA
		if(src->size < off + 2){
			encap_u1v(src, 2);
			sl = fread(src->buffer + src->size, 1, off + 2 - src->size, in);
			src->size += sl;
		}
		if(src->size < off + 2) return PGZF_FILETYPE_UNKNOWN;
		val = _bytes2num_pgzf(src->buffer + off, 2);
		off += 2;
		end = off + val;
		if(val > 0 && val < 4) return PGZF_FILETYPE_UNKNOWN;
		if(src->size < off + val){
			encap_u1v(src, val);
			sl = fread(src->buffer + src->size, 1, off + val - src->size, in);
			src->size += sl;
			if(src->size < off + val) return PGZF_FILETYPE_UNKNOWN;
		}
		//parse TAGs
		if(is_pgzf){
			while(off < end){
				si1 = src->buffer[off + 0];
				si2 = src->buffer[off + 1];
				sl = _bytes2num_pgzf(src->buffer + off + 2, 2);
				off += 4;
				if(si1 == 'Z' && si2 == 'C'){
					if(zcval) zcval[0] = _bytes2num_pgzf(src->buffer + off, sl);
				} else if(si1 == 'G' && si2 == 'C'){
					if(gcval) gcval[0] = _bytes2num_pgzf(src->buffer + off, sl);
				} else if(si1 == 'I' && si2 == 'X'){
					if(ixval) ixval[0] = sl;
					if(ixoff) ixoff[0] = off;
				}
				off += sl;
			}
		} else {
			off = end;
		}
	}
	if(xflag & 0x08){
		do {
			if(off < src->size){
				ch = src->buffer[off];
			} else {
				ch = getc(in);
				if(ch == -1){
					return PGZF_FILETYPE_UNKNOWN;
				}
				push_u1v(src, ch);
			}
			off ++;
		} while(ch);
	}
	if(xflag & 0x10){
		do {
			if(off < src->size){
				ch = src->buffer[off];
			} else {
				ch = getc(in);
				if(ch == -1){
					return PGZF_FILETYPE_UNKNOWN;
				}
				push_u1v(src, ch);
			}
			off ++;
		} while(ch);
	}
	if(xflag & 0x02){
		if(src->size < off + 2){
			encap_u1v(src, 2);
			sl = fread(src->buffer + src->size, 1, off + 2 - src->size, in);
			src->size += sl;
		}
		off += 2;
		if(src->size < off) return PGZF_FILETYPE_UNKNOWN;
	}
	*hoff = off;
	return is_pgzf? PGZF_FILETYPE_PGZF : PGZF_FILETYPE_GZ;
}

int pgzf_inflate_raw_core(z_stream *z, u1i *dst, u4i *dlen, u1i *src, u4i *slen, int flush){
	u4i dl, sl;
	int ret;
	ret = Z_OK;
	dl = *dlen;
	sl = *slen;
	z->avail_in = sl;
	z->next_in  = src;
	z->avail_out = dl;
	z->next_out  = dst;
	ret = inflate(z, flush);
	*dlen = dl - z->avail_out;
	*slen = sl - z->avail_in;
	return ret;
}

// src start just after gz_header, and include gz_tailer
int pgzf_inflate_core(u1i *dst, u4i *dlen, u1i *src, u4i slen, int check){
	z_stream Z, *z;
	u4i soff, dsz;
	uLong crc, rcr;
	int ret;
	z = &Z;
	ZEROS(z);
	inflateInit2(z, -15);
	z->avail_in  = slen - PGZF_TAIL_SIZE;
	z->next_in   = src;
	z->avail_out = *dlen;
	z->next_out  = dst;
	ret = inflate(z, Z_FINISH);
	*dlen -= z->avail_out;
	soff = slen - PGZF_TAIL_SIZE - z->avail_in;
	inflateEnd(z);
	if(check){
		if(soff + 8 > slen){
			fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			abort();
		}
		rcr = _bytes2num_pgzf(src + soff, 4);
		dsz = _bytes2num_pgzf(src + soff + 4, 4);
		if(dsz != *dlen){
			fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			abort();
		}
		crc = crc32(0L, Z_NULL, 0);
		crc = crc32(crc, dst, *dlen);
		if(crc != rcr){
			fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			abort();
		}
	}
	return 1;
}

thread_beg_func(pgz);
PGZF *pz;
u1v *dst, *src;
unsigned long crc;
u4i bufsize, hsize, rsize, zsize, dsz, ssz, next, i, j;
int ret;
pz  = pgz->pz;
dst = pgz->dst;
src = pgz->src;
thread_beg_loop(pgz);
if(pgz->task == PGZF_TASK_DEFLATE){
	if(!pgz->has) continue;
	clear_u1v(dst);
	if(pgz->blocktype == PGZF_BLOCKTYPE_BEG){
		zsize = compressBound(src->size);
		encap_u1v(dst, PGZF_BEG_HEAD_SIZE + zsize + PGZF_TAIL_SIZE);
		zsize = _zlib_raw_deflate_all(dst->buffer + PGZF_BEG_HEAD_SIZE, zsize, src->buffer, src->size, pgz->level);
		_gen_beg_pgzf_header(dst->buffer, PGZF_BEG_HEAD_SIZE + zsize + PGZF_TAIL_SIZE, 0);
		crc = crc32(0L, Z_NULL, 0);
		crc = crc32(crc, src->buffer, src->size);
		_gen_pgzf_tailer(dst->buffer + PGZF_BEG_HEAD_SIZE + zsize, crc, src->size);
		dst->size = PGZF_BEG_HEAD_SIZE + zsize + PGZF_TAIL_SIZE;
	} else if(pgz->blocktype == PGZF_BLOCKTYPE_DAT){
		zsize = compressBound(src->size);
		encap_u1v(dst, PGZF_DAT_HEAD_SIZE + zsize + PGZF_TAIL_SIZE);
		zsize = _zlib_raw_deflate_all(dst->buffer + PGZF_DAT_HEAD_SIZE, zsize, src->buffer, src->size, pgz->level);
		_gen_dat_pgzf_header(dst->buffer, PGZF_DAT_HEAD_SIZE + zsize + PGZF_TAIL_SIZE);
		crc = crc32(0L, Z_NULL, 0);
		crc = crc32(crc, src->buffer, src->size);
		_gen_pgzf_tailer(dst->buffer + PGZF_DAT_HEAD_SIZE + zsize, crc, src->size);
		dst->size = PGZF_DAT_HEAD_SIZE + zsize + PGZF_TAIL_SIZE;
	}
	while(pz->ridx != pgz->token){
		nano_sleep(1);
	}
	if(pgz->blocktype == PGZF_BLOCKTYPE_IDX){
		rsize = pz->blocks[0]->size * 2 * 4;
		hsize = PGZF_IDX_HEAD_SIZE + rsize;
		zsize = compressBound(0);
		encap_u1v(dst, hsize + zsize + PGZF_TAIL_SIZE);
		zsize = _zlib_raw_deflate_all(dst->buffer + hsize, zsize, src->buffer, 0, pgz->level); // empty content
		_gen_idx_pgzf_header(dst->buffer, hsize + zsize + PGZF_TAIL_SIZE, rsize);
		dsz = ssz = 0;
		j = PGZF_IDX_HEAD_SIZE;
		for(i=0;i<pz->blocks[0]->size;i++){
			dsz = pz->blocks[0]->buffer[i];
			ssz = pz->blocks[1]->buffer[i];
			_num2bytes_pgzf(dst->buffer + j, 4, dsz); j += 4;
			_num2bytes_pgzf(dst->buffer + j, 4, ssz); j += 4;
		}
#if DEBUG
		if(j != hsize){
			fflush(stdout); fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			abort();
		}
#endif
		pz->blocks[0]->size = 0;
		pz->blocks[1]->size = 0;
		_gen_pgzf_tailer(dst->buffer + hsize + zsize, 0, 0);
		dst->size = hsize + zsize + PGZF_TAIL_SIZE;
	} else {
		push_u8v(pz->blocks[0], pgz->dst->size);
		push_u8v(pz->blocks[1], pgz->src->size);
	}
	if(pz->verbose){
		fflush(stdout); fprintf(stderr, " -- compressed block: type=%d id=%d src=%d dst=%d --\n", pgz->blocktype, pgz->token, (u4i)pgz->src->size, (u4i)pgz->dst->size); fflush(stderr);
	}
	pgz->fileoff = ftell(pz->file);
	fwrite(pgz->dst->buffer, 1, pgz->dst->size, pz->file);
	pz->tot_out += pgz->dst->size;
	if(pgz->blocktype == PGZF_BLOCKTYPE_IDX){ // update BEG_HEADER
		b8i offset;
		offset = ftell(pz->file);
		fseek(pz->file, pz->lst_off + PGZF_BEG_HEAD_GC_OFFSET, SEEK_SET);
		zsize = pz->tot_out - pz->lst_out - pgz->dst->size;
		encap_u1v(pgz->dst, 4);
		_num2bytes_pgzf(pgz->dst->buffer, 4, zsize);
		if(pz->verbose){
			fflush(stdout); fprintf(stderr, " -- created compress group: offset=%lld,%lld length=%lld,%lld --\n", pz->lst_in, pz->lst_out, pz->tot_in - pz->lst_in, pz->tot_out - pz->lst_out - pgz->dst->size); fflush(stderr);
		}
		fwrite(pgz->dst->buffer, 1, 4, pz->file);
		fseek(pz->file, offset, SEEK_SET);
		pz->lst_off = offset; // in append mode, offset != pz->tot_out
		pz->lst_in  = pz->tot_in;
		pz->lst_out = pz->tot_out;
	}
	clear_u1v(pgz->dst);
	clear_u1v(pgz->src); pgz->soff = 0;
	recap_u1v(pgz->src, pz->bufsize);
	pgz->has = 0;
	pgz->blocktype = PGZF_BLOCKTYPE_NIL;
	pz->ridx ++;
} else if(pgz->task == PGZF_TASK_INFLATE){
	pgz->has = 0;
	pgz->doff = 0;
	clear_u1v(pgz->dst);
	while((pz->ridx % pz->ncpu) != UInt(pgz->t_idx)){
		nano_sleep(10);
		if(pz->error) break;
		if(pgz->eof == 2) break;
	}
	if((pgz->eof == 1 && pgz->soff >= pgz->src->size)){
		pz->ridx ++;
		break;
	}
	if(pz->error || pgz->eof == 2){
		break;
	}
	if(pz->rw_mode == PGZF_MODE_R){
		if(pgz->src->size){ // already loaded header
		} else if(pgz->eof == 0){
			pgz->soff = pgz->src->size = 0;
			if(pz->sels->size){
				u8i blkidx;
				if(pz->ridx >= pz->sels->size){
					pgz->eof = 1;
					pz->ridx ++;
					break;
				}
				blkidx = pz->sels->buffer[pz->ridx];
				if(blkidx >= pz->blocks[0]->size){
					pz->error = 1;
					pz->ridx ++;
					break;
				}
				fseek(pz->file, pz->blocks[0]->buffer[blkidx], SEEK_SET);
			}
			pgz->fileoff = ftell(pz->file);
			ret = _read_pgzf_header(pz->file, pgz->src, &pgz->soff, &pgz->zcval, &pgz->gcval, &pgz->ixval, &pgz->ixoff);
			if(pgz->src->size == 0){
				pgz->eof = 1;
				pz->ridx ++;
				break;
			}
			if(ret != PGZF_FILETYPE_PGZF){
				fprintf(stderr, " -- Error: not a PGZF format at %u block, in %s -- %s:%d --\n", pz->ridx, __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
				DEBUG_BREAK(1);
				pz->error = 1;
				pz->ridx ++;
				break;
			}
			pz->tot_in += pgz->src->size;
		}
		hsize = pgz->soff;
		encap_u1v(pgz->src, pgz->zcval - pgz->src->size);
		rsize = fread(pgz->src->buffer + hsize, 1, pgz->zcval - pgz->src->size, pz->file);
		if(rsize < pgz->zcval - pgz->src->size){
			fprintf(stderr, " -- Error: read %u < %u at %u block, in %s -- %s:%d --\n", UInt(pgz->src->size + rsize), pgz->zcval, pz->ridx, __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			pz->error = 1;
			pz->ridx ++;
			break;
		}
		pgz->src->size += rsize;
		pz->tot_in += rsize;
		next = pz->ridx ++;
		dsz = _bytes2num_pgzf(pgz->src->buffer + pgz->zcval - 4, 4);
		encap_u1v(pgz->dst, dsz);
		pgz->soff = 0;
		if(pgzf_inflate_core(pgz->dst->buffer, &dsz, pgz->src->buffer + hsize, pgz->zcval - hsize, 1) == 0){
			clear_u1v(pgz->src); pgz->soff = 0;
			pz->error = 1;
			break;
		}
		pgz->dst->size = dsz;
		if(pz->verbose){
			fflush(stdout); fprintf(stderr, " -- decompressed block: id=%d src=%d dst=%d --\n", next, (u4i)pgz->src->size, (u4i)pgz->dst->size); fflush(stderr);
		}
		clear_u1v(pgz->src); pgz->soff = 0;
	} else if(pz->rw_mode == PGZF_MODE_R_GZ){
		u4i bsz;
		bsz = 1024 * 1024;
		bufsize = pz->bufsize? pz->bufsize : PGZF_DEFAULT_BUFF_SIZE;
		encap_u1v(pgz->dst, bufsize);
		while(!pz->error){
			if(pgz->src->size == pgz->soff){
				pgz->soff = pgz->src->size = 0;
			}
			if(!pgz->eof && pgz->src->size < bsz){
				encap_u1v(pgz->src, bsz - pgz->src->size);
				rsize = fread(pgz->src->buffer + pgz->src->size, 1, bsz - pgz->src->size, pz->file);
				if(rsize < bsz - pgz->src->size){
					pgz->eof = 1;
				}
				pz->tot_in += rsize;
				pgz->src->size += rsize;
			}
			if(pgz->src->size == pgz->soff){
				break;
			}
			if(pz->step == 0){
				u4i tsz;
				tsz = pgz->src->size;
				ret = _read_pgzf_header(pz->file, pgz->src, &pgz->soff, &pgz->zcval, &pgz->gcval, &pgz->ixval, &pgz->ixoff);
				if(ret != PGZF_FILETYPE_GZ && ret != PGZF_FILETYPE_PGZF){
					if(pgz->src->size == pgz->soff){
						if(pgz->eof == 0) pgz->eof = 1;
					} else {
						fprintf(stderr, " -- failed in read gzip header, ret = %d in %s -- %s:%d --\n", ret, __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
						DEBUG_BREAK(1);
						pz->error = 1;
					}
					break;
				} else {
					pz->tot_in += pgz->src->size - tsz;
				}
				pz->step = 1;
				continue;
			} else if(pz->step == 2){
				if(pgz->src->size >= pgz->soff + PGZF_TAIL_SIZE){
					pgz->soff += PGZF_TAIL_SIZE;
					pz->step = 0;
					inflateReset(pz->z);
					continue;
				} else if(pgz->eof){
					pz->error = 2;
					DEBUG_BREAK(1);
					break;
				} else {
					memmove(pgz->src->buffer, pgz->src->buffer + pgz->soff, pgz->src->size - pgz->soff);
					pgz->src->size -= pgz->soff;
					pgz->soff = 0;
				}
			}
			while(pgz->dst->size < bufsize && pgz->soff < pgz->src->size){
				dsz = bufsize - pgz->dst->size;
				ssz = pgz->src->size - pgz->soff;
				ret = pgzf_inflate_raw_core(pz->z, pgz->dst->buffer + pgz->dst->size, &dsz, pgz->src->buffer + pgz->soff, &ssz, Z_NO_FLUSH);
				pgz->dst->size += dsz;
				pgz->soff += ssz;
				if(ret == Z_STREAM_END){
					pz->step = 2;
					break;
				} else if(ret != Z_OK){
					fprintf(stderr, " -- ZERROR: %d in %s -- %s:%d --\n", ret, __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
					DEBUG_BREAK(1);
					pz->error = 1;
					break;
				}
			}
			if(pgz->dst->size == bufsize){
				if(pgz->soff < pgz->src->size){
					if(pz->ncpu > 1){
						next = (pz->ridx + 1) % pz->ncpu;
						if(pz->srcs[next]->size != 0){
							fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
							abort();
						}
						append_array_u1v(pz->srcs[next], pgz->src->buffer + pgz->soff, pgz->src->size - pgz->soff);
						pgz->soff = pgz->src->size;
					} else {
						memmove(pgz->src->buffer, pgz->src->buffer + pgz->soff, pgz->src->size - pgz->soff);
					}
				}
				pgz->src->size -= pgz->soff;
				pgz->soff = 0;
				break;
			}
		}
		pz->ridx ++;
	} else if(pz->rw_mode == PGZF_MODE_R_UNKNOWN){
		bufsize = pz->bufsize? pz->bufsize : PGZF_DEFAULT_BUFF_SIZE;
		encap_u1v(pgz->dst, bufsize);
		if(pgz->src->size > bufsize){
			fprintf(stderr, " -- something wrong in %s -- %s:%d --\n", __FUNCTION__, __FILE__, __LINE__); fflush(stderr);
			pz->error = 1;
			pz->ridx ++;
			break;
		} else if(pgz->src->size){
			append_u1v(pgz->dst, pgz->src);
			clear_u1v(pgz->src); pgz->soff = 0;
		}
		rsize = fread(pgz->dst->buffer + pgz->dst->size, 1, bufsize - pgz->dst->size, pz->file);
		if(rsize < bufsize - pgz->dst->size){
			pgz->eof = 1;
		}
		pgz->dst->size += rsize;
		pz->tot_in += pgz->dst->size;
		pz->ridx ++;
	}
	pgz->has = pgz->ixoff? 0: 1;
}
thread_end_loop(pgz);
thread_end_func(pgz);

static inline PGZF* open_pgzf_writer(FILE *out, u4i buffer_size, int ncpu, int level){
	PGZF *pz;
	u4i i;
	b8i offset;
	thread_prepare(pgz);
	pz = malloc(sizeof(PGZF));
	if(ncpu < 1){
		get_linux_sys_info(NULL, NULL, &ncpu);
		if(ncpu < 1) ncpu = 8;
	}
	pz->ncpu = ncpu;
	pz->ridx = 0;
	pz->widx = 0;
	offset = ftell(out);
	if(offset == -1){
		pz->offset = 0;
		pz->seekable = 0;
	} else {
		pz->offset = offset;
		pz->seekable = 1;
	}
	pz->file = out;
	pz->ftype = PGZF_FILETYPE_PGZF;
	pz->error = 0;
	pz->verbose = 0;
	pz->grp_status = 1;
	pz->step = 0;
	pz->rw_mode = 1; // write
	if(buffer_size == 0) buffer_size = PGZF_DEFAULT_BUFF_SIZE;
	pz->bufsize = buffer_size;
	pz->grp_blocks = 8000;
	pz->blocks[0] = init_u8v(32);
	pz->blocks[1] = init_u8v(32);
	pz->sels = init_u8v(8);
	pz->z = NULL;
	pz->dsts = calloc(pz->ncpu, sizeof(u1v*));
	for(i=0;i<pz->ncpu;i++){
		pz->dsts[i] = init_u1v(pz->bufsize);
	}
	pz->srcs = calloc(pz->ncpu, sizeof(u1v*));
	for(i=0;i<pz->ncpu;i++){
		pz->srcs[i] = init_u1v(pz->bufsize);
	}
	pz->tmp = init_u1v(32);
	pz->tot_in  = 0;
	pz->tot_out = 0;
	pz->lst_in  = 0;
	pz->lst_out = 0;
	pz->lst_off = pz->offset;
	if(level == 0) level = Z_DEFAULT_COMPRESSION; // disable level 0, set to default level 6
	thread_beg_init(pgz, pz->ncpu);
	pgz->pz = pz;
	pgz->zcval = 0;
	pgz->gcval = 0;
	pgz->ixval = 0;
	pgz->ixoff = 0;
	pgz->soff = 0;
	pgz->fileoff = 0;
	pgz->doff = 0;
	pgz->dst = pz->dsts[pgz->t_idx];
	pgz->src = pz->srcs[pgz->t_idx];
	pgz->token = 0;
	pgz->level = level;
	pgz->eof = 0;
	pgz->task = PGZF_TASK_NULL;
	pgz->has = 0;
	pgz->blocktype = PGZF_BLOCKTYPE_NIL;
	thread_end_init(pgz);
	thread_export(pgz, pz);
	return pz;
}

static inline int write_index_pgzf(PGZF *pz){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(!pz->seekable) return 0;
	if(pz->grp_status) return 0;
	thread_beg_operate(pgz, pz->widx % pz->ncpu);
	thread_wait(pgz);
	pgz->task = PGZF_TASK_DEFLATE;
	pgz->blocktype = PGZF_BLOCKTYPE_IDX;
	pgz->token = pz->widx;
	pgz->has = 1; // len can be zero
	thread_wake(pgz);
	pz->grp_status = 1;
	pz->widx ++;
	return 1;
}

static inline size_t write_pgzf(PGZF *pz, void *dat, size_t len){
	size_t off, cnt;
	thread_prepare(pgz);
	thread_import(pgz, pz);
	off = 0;
	while(off < len){
		thread_beg_operate(pgz, pz->widx % pz->ncpu);
		thread_wait(pgz);
		cnt = num_min(len - off, pz->bufsize - pgz->src->size);
		append_array_u1v(pgz->src, dat + off, cnt);
		off += cnt;
		if(pgz->src->size == pz->bufsize){
			pz->tot_in += pgz->src->size;
			if(pz->grp_status){
				pgz->blocktype = PGZF_BLOCKTYPE_BEG;
				pz->grp_status = 0;
			} else {
				pgz->blocktype = PGZF_BLOCKTYPE_DAT;
			}
			pgz->task = PGZF_TASK_DEFLATE;
			pgz->token = pz->widx;
			pgz->has = 1;
			thread_wake(pgz);
			pz->widx ++;
			if((pz->widx % pz->grp_blocks) == 0){
				write_index_pgzf(pz);
			}
		}
	}
	return len;
}

static inline size_t write_block_pgzf(PGZF *pz, void *dat, size_t len){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	thread_beg_operate(pgz, pz->widx % pz->ncpu);
	thread_wait(pgz);
	append_array_u1v(pgz->src, dat, len);
	pz->tot_in += pgz->src->size;
	pgz->task = PGZF_TASK_DEFLATE;
	if(pz->grp_status){
		pgz->blocktype = PGZF_BLOCKTYPE_BEG;
		pz->grp_status = 0;
	} else {
		pgz->blocktype = PGZF_BLOCKTYPE_DAT;
	}
	pgz->token = pz->widx;
	pgz->has = 1; // len can be zero
	thread_wake(pgz);
	pz->widx ++;
	if((pz->widx % pz->grp_blocks) == 0){
		write_index_pgzf(pz);
	}
	return len;
}

static inline void _end_pgzf_writer(PGZF *pz){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	thread_beg_operate(pgz, pz->widx % pz->ncpu);
	if(pgz->src->size){ // will force to write un-full block
		pz->tot_in += pgz->src->size;
		pgz->task = PGZF_TASK_DEFLATE;
		if(pz->grp_status){
			pgz->blocktype = PGZF_BLOCKTYPE_BEG;
			pz->grp_status = 0;
		} else {
			pgz->blocktype = PGZF_BLOCKTYPE_DAT;
		}
		pgz->token = pz->widx;
		pgz->has = 1;
		thread_wake(pgz);
		pz->widx ++;
		if((pz->widx % pz->grp_blocks) == 0){
			write_index_pgzf(pz);
		}
	}
	write_index_pgzf(pz);
}

static inline PGZF* open_pgzf_reader(FILE *in, u4i bufsize, int ncpu){
	PGZF *pz;
	b8i offset;
	u4i i, hoff, zcval;
	thread_prepare(pgz);
	pz = malloc(sizeof(PGZF));
	if(ncpu < 1){
		get_linux_sys_info(NULL, NULL, &ncpu);
		if(ncpu < 1) ncpu = 8;
	}
	pz->ncpu = ncpu;
	pz->ridx = 0;
	pz->widx = 0;
	offset = ftell(in);
	if(offset == -1){
		pz->offset = 0;
		pz->seekable = 0;
	} else {
		pz->offset = offset;
		pz->seekable = 1;
	}
	pz->file = in;
	pz->grp_status = 1;
	pz->grp_blocks = 0;
	pz->error = 0;
	pz->step = 0;
	pz->verbose = 0;
	pz->dsts = calloc(pz->ncpu, sizeof(u1v*));
	pz->srcs = calloc(pz->ncpu, sizeof(u1v*));
	pz->tmp = init_u1v(32);
	pz->tot_in  = 0;
	pz->tot_out = 0;
	pz->lst_in  = 0;
	pz->lst_out = 0;
	pz->lst_off = pz->offset;
	pz->blocks[0] = init_u8v(32);
	pz->blocks[1] = init_u8v(32);
	pz->sels = init_u8v(8);
	// recognize PGZF
	hoff = 0;
	pz->srcs[0] = init_u1v(1024);
	pz->ftype = _read_pgzf_header(pz->file, pz->srcs[0], &hoff, &zcval, NULL, NULL, NULL);
	pz->tot_in = pz->srcs[0]->size;
	switch(pz->ftype){
		case PGZF_FILETYPE_GZ: pz->step = 1; pz->rw_mode = PGZF_MODE_R_GZ; break;
		case PGZF_FILETYPE_PGZF: pz->rw_mode = PGZF_MODE_R; break;
		default:
			fprintf(stderr, " ** WARNNING: input file is not in gzip format **\n");
			pz->rw_mode = PGZF_MODE_R_UNKNOWN; break;
	}
	if(pz->rw_mode == PGZF_MODE_R){
		pz->z = NULL;
		if(pz->seekable){
			// read the uncompressed size
			u8i foff;
			foff = ftell(pz->file);
			if(fseek(pz->file, pz->offset + zcval - 4, SEEK_SET) == -1){
				fprintf(stderr, " ** ERROR: failed to read uncompress block size in the first block ERR(1) **\n");
				return NULL;
			}
			if(fread(&pz->bufsize, 4, 1, pz->file) == 0){
				fprintf(stderr, " ** ERROR: failed to read uncompress block size in the first block ERR(2) **\n");
				return NULL;
			}
			if(fseek(pz->file, foff, SEEK_SET) == -1){
				fprintf(stderr, " ** ERROR: failed to read uncompress block size in the first block ERR(3) **\n");
				return NULL;
			}
		} else {
			pz->bufsize = bufsize;
		}
	} else if(pz->rw_mode == PGZF_MODE_R_GZ){
		pz->z = calloc(1, sizeof(z_stream));
		inflateInit2(pz->z, -15);
		pz->bufsize = bufsize;
	} else {
		pz->z = NULL;
		pz->bufsize = bufsize;
	}
	if(pz->bufsize == 0) pz->bufsize = PGZF_DEFAULT_BUFF_SIZE;
	for(i=0;i<pz->ncpu;i++){
		pz->dsts[i] = init_u1v(pz->bufsize);
	}
	if(pz->bufsize > pz->srcs[0]->size){
		encap_u1v(pz->srcs[0], pz->bufsize - pz->srcs[0]->size);
	}
	for(i=1;i<pz->ncpu;i++){
		pz->srcs[i] = init_u1v(pz->bufsize);
	}
	thread_beg_init(pgz, pz->ncpu);
	pgz->pz = pz;
	pgz->zcval = pgz->t_idx? 0 : zcval;
	pgz->gcval = 0;
	pgz->ixval = 0;
	pgz->ixoff = 0;
	pgz->soff = pgz->t_idx? 0 : hoff;
	pgz->fileoff = 0;
	pgz->doff = 0;
	pgz->src = pz->srcs[pgz->t_idx];
	pgz->dst = pz->dsts[pgz->t_idx];
	pgz->level = Z_DEFAULT_COMPRESSION; // useless in inflating
	pgz->has = 0;
	pgz->eof = 0;
	pgz->task = PGZF_TASK_INFLATE;
	thread_end_init(pgz);
	//thread_beg_operate(pgz, 0);
	//thread_wake(pgz);
	//thread_wake_all(pgz);
	thread_export(pgz, pz);
	return pz;
}

static inline void reset_pgzf_reader(PGZF *pz){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	thread_beg_iter(pgz);
	pgz->eof = 2;
	thread_wait(pgz);
	thread_set_idle(pgz);
	clear_u1v(pgz->src); pgz->soff = 0;
	clear_u1v(pgz->dst);
	pgz->zcval = 0;
	pgz->gcval = 0;
	pgz->ixval = 0;
	pgz->ixoff = 0;
	pgz->soff = 0;
	pgz->doff = 0;
	pgz->task = PGZF_TASK_INFLATE;
	pgz->has = 0;
	pgz->eof = 0;
	thread_end_iter(pgz);
	pz->ridx = 0;
	pz->widx = 0;
	pz->error = 0;
	pz->step = 0;
	pz->tot_in  = 0;
	pz->tot_out = 0;
	//thread_beg_operate(pgz, 0);
	//thread_wake(pgz);
}

static inline int load_index_pgzf(PGZF *pz){
	b8i off, tot, blks[2];
	u4i i, cnts[2];
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(pz->rw_mode != PGZF_MODE_R) return 0;
	if(!pz->seekable) return 0;
	if(pz->blocks[0]->size) return 1;
	reset_pgzf_reader(pz);
	thread_beg_operate(pgz, 0);
	fseek(pz->file, 0, SEEK_END);
	tot = ftell(pz->file);
	off = 0;
	blks[0] = blks[1] = 0;
	while(off < tot){
		fseek(pz->file, off, SEEK_SET);
		clear_u1v(pgz->src); pgz->soff = 0;
		_read_pgzf_header(pz->file, pgz->src, &pgz->soff, &pgz->zcval, &pgz->gcval, &pgz->ixval, &pgz->ixoff);
		if(pgz->gcval == 0){
			break;
		}
		off += pgz->gcval;
		fseek(pz->file, off, SEEK_SET);
		clear_u1v(pgz->src); pgz->soff = 0;
		_read_pgzf_header(pz->file, pgz->src, &pgz->soff, &pgz->zcval, &pgz->gcval, &pgz->ixval, &pgz->ixoff);
		for(i=0;i<pgz->ixval;i+=8){
			push_u8v(pz->blocks[0], blks[0]);
			push_u8v(pz->blocks[1], blks[1]);
			cnts[0] = _bytes2num_pgzf(pgz->src->buffer + pgz->ixoff + i + 0, 4);
			cnts[1] = _bytes2num_pgzf(pgz->src->buffer + pgz->ixoff + i + 4, 4);
			blks[0] += cnts[0];
			blks[1] += cnts[1];
		}
		blks[0] += pgz->zcval;
		blks[1] += 0;
		off += pgz->zcval;
	}
	clear_u1v(pgz->src); pgz->soff = 0; pgz->ixval =  pgz->ixoff = 0;
	push_u8v(pz->blocks[0], blks[0]);
	push_u8v(pz->blocks[1], blks[1]);
	fseek(pz->file, 0, SEEK_SET);
	return (off == tot);
}

// MUST call before any read operation
static inline int select_blocks_pgzf_reader(PGZF *pz, u8i *blks, u4i cnt){
	if(load_index_pgzf(pz) == 0) return 0;
	append_array_u8v(pz->sels, blks, cnt);
	return 1;
}

static inline size_t read_pgzf(PGZF *pz, void *dat, size_t len){
	size_t off;
	u4i nrun;
	thread_prepare(pgz);
	thread_import(pgz, pz);
	nrun = 0;
	for(off=0;off<len;){
		thread_beg_operate(pgz, pz->widx % pz->ncpu);
		int state = thread_wait(pgz);
		if(state == THREAD_STATE_IDLE){ // hasn't start
			thread_wake(pgz);
			state = thread_wait(pgz);
		}
		if(pz->verbose){
			fflush(stdout); fprintf(stderr, " -- read: id=%d(%d,%d) tot=[%d-%d] fileoff=%llu off=%llu len=%llu --\n", pz->widx, thread_index(pgz), state, (u4i)pgz->doff, (u4i)pgz->dst->size, pgz->fileoff, (u8i)off, (u8i)len); fflush(stderr);
		}
		if(pz->error) break;
		if(len - off < pgz->dst->size - pgz->doff){
			if(dat) memcpy(dat + off, pgz->dst->buffer + pgz->doff, len - off);
			pz->tot_out += len - off;
			pgz->doff += len - off;
			off = len;
			break;
		} else if(pgz->dst->size){
			if(dat) memcpy(dat + off, pgz->dst->buffer + pgz->doff, pgz->dst->size - pgz->doff);
			pz->tot_out += pgz->dst->size - pgz->doff;
			off += pgz->dst->size - pgz->doff;
			pgz->doff = pgz->dst->size;
		} else if(pgz->eof){
			nrun ++;
			if(nrun >= pz->ncpu){
				break;
			}
		}
		thread_wake(pgz);
		pz->widx ++;
	}
	return off;
}

// -1: mean EOF/ERR
static inline ssize_t read_block_pgzf(PGZF *pz, u1v *dat){
	u8i inc;
	u4i nrun, finish;
	thread_prepare(pgz);
	thread_import(pgz, pz);
	nrun = inc = finish = 0;
	while(!finish){
		thread_beg_operate(pgz, pz->widx % pz->ncpu);
		thread_wait(pgz);
		if(pz->error) return -1;
		if(pgz->dst->size || pgz->has){
			inc = pgz->dst->size - pgz->doff;
			if(dat){
				encap_u1v(dat, inc);
				memcpy(dat->buffer + dat->size, pgz->dst->buffer + pgz->doff, inc);
				dat->size += inc;
			}
			pz->tot_out += inc;
			pgz->doff = pgz->dst->size;
			finish = 1;
		} else if(pgz->eof){
			nrun ++;
			if(nrun >= pz->ncpu){
				return -1;
			}
		}
		thread_wake(pgz);
		pz->widx ++;
	}
	return inc;
}

static inline b8i tell_pgzf(PGZF *pz){
	return (pz->rw_mode == PGZF_MODE_W)? pz->tot_in : pz->tot_out;
}

static inline int eof_pgzf(PGZF *pz){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(pz->error) return 1;
	thread_beg_iter(pgz);
	if(!pgz->eof || pgz->doff < pgz->dst->size) return 0;
	thread_end_iter(pgz);
	return 1;
}

static inline int seek_pgzf(PGZF *pz, b8i offset, int whence){
	b8i tot, zoff, uoff;
	u4i idx;
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(!pz->seekable) return -1;
	if(pz->rw_mode == PGZF_MODE_R_UNKNOWN){
		reset_pgzf_reader(pz);
		if(fseek(pz->file, offset, whence) == -1) return -1;
		pz->tot_out = pz->tot_in = ftell(pz->file);
		return 0;
	} else if(pz->rw_mode != PGZF_MODE_R) return -1;
	if(whence == SEEK_CUR){
		offset += pz->tot_out;
	}
	if(pz->blocks[0]->size == 0){
		load_index_pgzf(pz);
	} else {
		reset_pgzf_reader(pz);
	}
	if(pz->blocks[0]->size == 0){
		return -1;
	} else {
		tot = pz->blocks[1]->buffer[pz->blocks[1]->size - 1];
		if(whence == SEEK_END){
			offset = tot - offset;
		}
		if(offset < 0) offset = 0;
		if(offset > tot) offset = tot;
		bsearch_array(pz->blocks[1]->buffer, pz->blocks[1]->size, u8i, idx, EXPR(a < (u8i)offset));
		if(pz->blocks[1]->buffer[idx] == (u8i)offset){
			zoff = pz->blocks[0]->buffer[idx];
			uoff = pz->blocks[1]->buffer[idx];
		} else {
			zoff = pz->blocks[0]->buffer[idx - 1];
			uoff = pz->blocks[1]->buffer[idx - 1];
		}
	}
	fseek(pz->file, zoff, SEEK_SET);
	pz->tot_in  = zoff;
	pz->tot_out = uoff;
	if(uoff < offset){
		read_pgzf(pz, NULL, offset - uoff);
	}
	return 0;
}

static inline int seek_block_pgzf(PGZF *pz, b8i block_idx){
	b8i tot, zoff, uoff;
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(!pz->seekable) return -1;
	if(pz->rw_mode != PGZF_MODE_R) return -1;
	if(pz->blocks[0]->size == 0){
		load_index_pgzf(pz);
	} else {
		reset_pgzf_reader(pz);
	}
	if(pz->blocks[0]->size == 0){
		return -1;
	}
	tot = pz->blocks[1]->size - 1;
	if(block_idx < 0) block_idx = tot + block_idx;
	if(block_idx > tot) block_idx = tot;
	zoff = pz->blocks[0]->buffer[block_idx];
	uoff = pz->blocks[1]->buffer[block_idx];
	fseek(pz->file, zoff, SEEK_SET);
	pz->tot_in  = zoff;
	pz->tot_out = uoff;
	return 0;
}

static inline void close_pgzf(PGZF *pz){
	thread_prepare(pgz);
	thread_import(pgz, pz);
	if(pz->rw_mode == PGZF_MODE_W){
		_end_pgzf_writer(pz);
	}
	thread_beg_iter(pgz);
	pgz->eof = 2;
	thread_end_iter(pgz);
	thread_beg_close(pgz);
	free_u1v(pgz->dst);
	free_u1v(pgz->src);
	thread_end_close(pgz);
	free(pz->dsts);
	free(pz->srcs);
	free_u1v(pz->tmp);
	switch(pz->rw_mode){
		case PGZF_MODE_W: fflush(pz->file); break;
		case PGZF_MODE_R: break;
		case PGZF_MODE_R_GZ:
			if(pz->z){
				inflateEnd(pz->z);
				free(pz->z);
			}
			break;
	}
	free_u8v(pz->blocks[0]);
	free_u8v(pz->blocks[1]);
	free_u8v(pz->sels);
	free(pz);
}

static inline ssize_t read_pgzf4filereader(void *obj, char *dat, size_t len){ return read_pgzf((PGZF*)obj, dat, len); }
static inline int close_pgzf4filereader(void *obj){
	PGZF *pz;
	FILE *file;
	pz = (PGZF*)obj;
	file = pz->file;
	close_pgzf(pz);
	if(file != stdin){
		fclose(file);
	}
	return 0;
}

static inline size_t write_pgzf4filewriter(void *obj, void *dat, size_t len){ return write_pgzf((PGZF*)obj, dat, len); }
static inline void close_pgzf4filewriter(void *obj){
	PGZF *pz;
	FILE *file;
	pz = (PGZF*)obj;
	file = pz->file;
	close_pgzf(pz);
	if(file != stdout){
		fclose(file);
	}
}

static inline ssize_t pgzf_io_read(void *obj, char *buffer, size_t size){
	PGZF *pz;
	pz = (PGZF*)obj;
	return read_pgzf(pz, (void*)buffer, size);
}

static inline ssize_t pgzf_io_write(void *obj, const char *buffer, size_t size){
	PGZF *pz;
	pz = (PGZF*)obj;
	return write_pgzf(pz, (void*)buffer, size);
}

static inline int pgzf_io_seek(void *obj, off64_t *pos, int whence){
	PGZF *pz;
	pz = (PGZF*)obj;
	if(seek_pgzf(pz, pos[0], whence) == -1) return -1;
	pos[0] = pz->tot_out;
	return 0;
}

static inline int pgzf_io_close(void *obj){
	PGZF *pz;
	FILE *file;
	pz = (PGZF*)obj;
	file = pz->file;
	close_pgzf(pz);
	close_file(file);
	return 0;
}

static const cookie_io_functions_t pgzf_io_funs = { pgzf_io_read, pgzf_io_write, pgzf_io_seek, pgzf_io_close };

static inline FILE* asfile_pgzf(PGZF *pz, char *rw_mode){
	return fopencookie(pz, rw_mode, pgzf_io_funs);
}

static inline FILE* fopen_pgzf_reader(char *prefix, char *suffix, u4i bufsize, int ncpu){
	PGZF *pz;
	FILE *in;
	in = open_file_for_read(prefix, suffix);
	pz = open_pgzf_reader(in, bufsize, ncpu);
	return asfile_pgzf(pz, "r");
}

static inline FILE* fopen_pgzf_writer(char *prefix, char *suffix, u4i bufsize, int ncpu, int level){
	PGZF *pz;
	FILE *out;
	out = open_file_for_write(prefix, suffix, 1);
	pz = open_pgzf_writer(out, bufsize, ncpu, level);
	return asfile_pgzf(pz, "w");
}

#endif
