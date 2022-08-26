#ifndef __SORT_RJ_H
#define __SORT_RJ_H

#include <stdio.h>
#include <stdlib.h>

#define cmp_2nums_proc(a, b) if((a) < (b)) return -1; else if((a) > (b)) return 1;
#define num_cmp_script(e1, e2, obj, val_macro) ((val_macro(e1, obj) == val_macro(e2, obj))? 0 : ((val_macro(e1, obj) < val_macro(e2, obj))? -1 : 1))
#define cmpgt_2nums_proc(a, b) if((a) < (b)) return 0; else if((a) > (b)) return 1;
#define cmpeq_2nums_proc(a, b) if((a) < (b)) return 0; else if((a) > (b)) return 0;

#define define_bubble_sort(name, e_type, is_greater_func)	\
static inline void name(e_type* list, size_t size, void *ref){	\
	size_t i, j, n;	\
	e_type t;	\
	i = 0;	\
	while(i < size){	\
		n = 0;	\
		for(j=size-1;j>i;j--){	\
			if(is_greater_func(list[j-1], list[j], ref) > 0){	\
				t = list[j-1]; list[j-1] = list[j]; list[j] = t;	\
				n = 1;	\
			}	\
		}	\
		if(n == 0) break;	\
		i ++;	\
	}	\
	if(ref == ref) return;	\
}

#define bubble_sort_array(rs, size, e_type, is_a_greater_than_b)	\
do {	\
	size_t bubble_i, bubble_j, bubble_n, bubble_size;	\
	e_type a, b;	\
	bubble_size = size;	\
	for(bubble_i=0;bubble_i<bubble_size;bubble_i++){	\
		bubble_n = 0;	\
		for(bubble_j=bubble_size-1;bubble_j>bubble_i;bubble_j--){	\
			a = (rs)[bubble_j - 1];	\
			b = (rs)[bubble_j];	\
			if((int)(is_a_greater_than_b) > 0){	\
				(rs)[bubble_j] = a; (rs)[bubble_j - 1] = b;	\
				bubble_n = 1;	\
			}	\
		}	\
		if(bubble_n == 0) break;	\
	}	\
} while(0)

#define divide_array(rs_ary, rs_size, e_type, is_a_greater_than_b, ret_val)	\
do {	\
	e_type *_rs;	\
	_rs = (e_type*)(rs_ary);	\
	size_t s, e, i, j, m;	\
	e_type p, t, a, b;	\
	if((rs_size) < 2){ (ret_val) = 0; break; }	\
	{	\
		s = 0;	\
		e = (rs_size) - 1;	\
		m = s + (e - s) / 2;	\
		a = _rs[s]; b = _rs[m];	\
		if(is_a_greater_than_b){ t = _rs[s]; _rs[s] = _rs[m]; _rs[m] = t; }	\
		a = _rs[m]; b = _rs[e];	\
		if(is_a_greater_than_b){	\
			t = _rs[e]; _rs[e] = _rs[m]; _rs[m] = t;	\
			a = _rs[s]; b = _rs[m];	\
			if(is_a_greater_than_b){ t = _rs[s]; _rs[s] = _rs[m]; _rs[m] = t; }	\
		}	\
		p = _rs[m];	\
		i = s + 1; j = e - 1;	\
		while(1){	\
			a = p;	\
			while(b = _rs[i], (is_a_greater_than_b)) i ++;	\
			b = p;	\
			while(a = _rs[j], (is_a_greater_than_b)) j --;	\
			if(i < j){	\
				t = _rs[i]; _rs[i] = _rs[j]; _rs[j] = t;	\
				i ++; j --;	\
			} else break;	\
		}	\
		if(i == j){ i ++; j --; }	\
		(ret_val) = i;	\
	}	\
} while(0)

#define indexsort_array(rs_size, is_a_greater_than_b, swap_expr)	\
do {	\
	size_t _qsort_n;	\
	_qsort_n = rs_size;	\
	size_t _qsort_s, _qsort_e, _qsort_i, _qsort_j, _qsort_m, _qsort_stack[64][2], _qsort_x, a, b;	\
	if(_qsort_n < 2) break;	\
	_qsort_x = 0;	\
	_qsort_stack[_qsort_x][0] = 0; _qsort_stack[_qsort_x][1] = _qsort_n - 1; _qsort_x ++;	\
	while(_qsort_x){	\
		_qsort_x --; _qsort_s = _qsort_stack[_qsort_x][0]; _qsort_e = _qsort_stack[_qsort_x][1];	\
		_qsort_m = _qsort_s + (_qsort_e - _qsort_s) / 2;	\
		a = _qsort_s; b = _qsort_m;	\
		if(is_a_greater_than_b){ swap_expr; }	\
		a = _qsort_m; b = _qsort_e;	\
		if(is_a_greater_than_b){	\
			swap_expr;	\
			a = _qsort_s; b = _qsort_m;	\
			if(is_a_greater_than_b){ swap_expr; }	\
		}	\
		_qsort_i = _qsort_s + 1; _qsort_j = _qsort_e - 1;	\
		while(1){	\
			a = _qsort_m;	\
			while(b = _qsort_i, (is_a_greater_than_b)) _qsort_i ++;	\
			b = _qsort_m;	\
			while(a = _qsort_j, (is_a_greater_than_b)) _qsort_j --;	\
			if(_qsort_i < _qsort_j){	\
				if(_qsort_m == _qsort_i) _qsort_m = _qsort_j;	\
				else if(_qsort_m == _qsort_j) _qsort_m = _qsort_i;	\
				a = _qsort_i; b = _qsort_j; \
				swap_expr; \
				_qsort_i ++; _qsort_j --;	\
			} else break;	\
		}	\
		if(_qsort_i == _qsort_j){ _qsort_i ++; _qsort_j --; }	\
		if(_qsort_j - _qsort_s > _qsort_e - _qsort_i){	\
			if(_qsort_s + 4 < _qsort_j){ _qsort_stack[_qsort_x][0] = _qsort_s; _qsort_stack[_qsort_x][1] = _qsort_j; _qsort_x ++; }	\
			if(_qsort_i + 4 < _qsort_e){ _qsort_stack[_qsort_x][0] = _qsort_i; _qsort_stack[_qsort_x][1] = _qsort_e; _qsort_x ++; }	\
		} else {	\
			if(_qsort_i + 4 < _qsort_e){ _qsort_stack[_qsort_x][0] = _qsort_i; _qsort_stack[_qsort_x][1] = _qsort_e; _qsort_x ++; }	\
			if(_qsort_s + 4 < _qsort_j){ _qsort_stack[_qsort_x][0] = _qsort_s; _qsort_stack[_qsort_x][1] = _qsort_j; _qsort_x ++; }	\
		}	\
	}	\
	for(_qsort_i=0;_qsort_i<_qsort_n;_qsort_i++){	\
		_qsort_x = 0;	\
		for(_qsort_j=_qsort_n-1;_qsort_j>_qsort_i;_qsort_j--){	\
			a = _qsort_j - 1; b = _qsort_j;	\
			if(is_a_greater_than_b){ swap_expr; _qsort_x = 1; }	\
		}	\
		if(_qsort_x == 0) break;	\
	}	\
} while(0)

#define sort_array_adv(rs_size, is_a_greater_than_b, swap_expr) indexsort_array(rs_size, is_a_greater_than_b, swap_expr)

#define sort_array(rs_ary, rs_size, e_type, is_a_greater_than_b)	\
do {	\
	e_type *_qsort_rs;	\
	_qsort_rs = (e_type*)(rs_ary);	\
	size_t _qsort_n;	\
	_qsort_n = rs_size;	\
	size_t _qsort_s, _qsort_e, _qsort_i, _qsort_j, _qsort_m, _qsort_stack[64][2], _qsort_x;	\
	e_type a;	\
	e_type b;	\
	e_type _qsort_t;	\
	e_type _qsort_p;	\
	if(_qsort_n < 2) break;	\
	_qsort_x = 0;	\
	_qsort_stack[_qsort_x][0] = 0; _qsort_stack[_qsort_x][1] = _qsort_n - 1; _qsort_x ++;	\
	while(_qsort_x){	\
		_qsort_x --; _qsort_s = _qsort_stack[_qsort_x][0]; _qsort_e = _qsort_stack[_qsort_x][1];	\
		_qsort_m = _qsort_s + (_qsort_e - _qsort_s) / 2;	\
		a = _qsort_rs[_qsort_s]; b = _qsort_rs[_qsort_m];	\
		if(is_a_greater_than_b){ _qsort_t = _qsort_rs[_qsort_s]; _qsort_rs[_qsort_s] = _qsort_rs[_qsort_m]; _qsort_rs[_qsort_m] = _qsort_t; }	\
		a = _qsort_rs[_qsort_m]; b = _qsort_rs[_qsort_e];	\
		if(is_a_greater_than_b){	\
			_qsort_t = _qsort_rs[_qsort_e]; _qsort_rs[_qsort_e] = _qsort_rs[_qsort_m]; _qsort_rs[_qsort_m] = _qsort_t;	\
			a = _qsort_rs[_qsort_s]; b = _qsort_rs[_qsort_m];	\
			if(is_a_greater_than_b){	\
				_qsort_t = _qsort_rs[_qsort_s]; _qsort_rs[_qsort_s] = _qsort_rs[_qsort_m]; _qsort_rs[_qsort_m] = _qsort_t;	\
			}	\
		}	\
		_qsort_p = _qsort_rs[_qsort_m];	\
		_qsort_i = _qsort_s + 1; _qsort_j = _qsort_e - 1;	\
		while(1){	\
			a = _qsort_p;	\
			while(b = _qsort_rs[_qsort_i], (is_a_greater_than_b)) _qsort_i ++;	\
			b = _qsort_p;	\
			while(a = _qsort_rs[_qsort_j], (is_a_greater_than_b)) _qsort_j --;	\
			if(_qsort_i < _qsort_j){	\
				_qsort_t = _qsort_rs[_qsort_i]; _qsort_rs[_qsort_i] = _qsort_rs[_qsort_j]; _qsort_rs[_qsort_j] = _qsort_t;	\
				_qsort_i ++; _qsort_j --;	\
			} else break;	\
		}	\
		if(_qsort_i == _qsort_j){ _qsort_i ++; _qsort_j --; }	\
		if(_qsort_j - _qsort_s > _qsort_e - _qsort_i){	\
			if(_qsort_s + 4 < _qsort_j){ _qsort_stack[_qsort_x][0] = _qsort_s; _qsort_stack[_qsort_x][1] = _qsort_j; _qsort_x ++; }	\
			if(_qsort_i + 4 < _qsort_e){ _qsort_stack[_qsort_x][0] = _qsort_i; _qsort_stack[_qsort_x][1] = _qsort_e; _qsort_x ++; }	\
		} else {	\
			if(_qsort_i + 4 < _qsort_e){ _qsort_stack[_qsort_x][0] = _qsort_i; _qsort_stack[_qsort_x][1] = _qsort_e; _qsort_x ++; }	\
			if(_qsort_s + 4 < _qsort_j){ _qsort_stack[_qsort_x][0] = _qsort_s; _qsort_stack[_qsort_x][1] = _qsort_j; _qsort_x ++; }	\
		}	\
	}	\
	for(_qsort_i=0;_qsort_i<_qsort_n;_qsort_i++){	\
		_qsort_x = 0;	\
		for(_qsort_j=_qsort_n-1;_qsort_j>_qsort_i;_qsort_j--){	\
			a = _qsort_rs[_qsort_j - 1]; b = _qsort_rs[_qsort_j];	\
			if(is_a_greater_than_b){	\
				_qsort_t = _qsort_rs[_qsort_j - 1]; _qsort_rs[_qsort_j - 1] = _qsort_rs[_qsort_j]; _qsort_rs[_qsort_j] = _qsort_t;	\
				_qsort_x = 1;	\
			}	\
		}	\
		if(_qsort_x == 0) break;	\
	}	\
} while(0)

// Must #include "thread.h" and "list.h"
#define psort_array(rs_ary, rs_size, e_type, ncpu, is_a_greater_than_b)	\
do {	\
	thread_beg_def(psrt);	\
	e_type *rs;	\
	size_t beg, end, div;	\
	int divide;	\
	thread_end_def(psrt);	\
	thread_beg_func_inline(psrt);	\
	thread_beg_loop(psrt);	\
	if(psrt->divide){	\
		divide_array(psrt->rs + psrt->beg, psrt->end - psrt->beg, e_type, is_a_greater_than_b, psrt->div);	\
		psrt->div += psrt->beg;	\
	} else {	\
		sort_array(psrt->rs + psrt->beg, psrt->end - psrt->beg, e_type, is_a_greater_than_b);	\
	}	\
	thread_end_loop(psrt);	\
	thread_end_func(psrt);	\
	thread_preprocess(psrt);	\
	e_type *_rs;	\
	_rs = (e_type*)(rs_ary);	\
	size_t _qsort_n, _min_blk_size;	\
	_qsort_n = rs_size;	\
	_min_blk_size = _qsort_n / ncpu / 8;	\
	if(_min_blk_size < 1024) _min_blk_size = 1024;	\
	if(_qsort_n < ((uint32_t)(ncpu)) * 32){	\
		sort_array(rs_ary, rs_size, e_type, is_a_greater_than_b);	\
		break;	\
	}	\
	thread_beg_init(psrt, (int)(ncpu));	\
	psrt->rs = _rs; psrt->beg = psrt->end = psrt->div = 0; psrt->divide = 0;	\
	thread_end_init(psrt);	\
	u64list *stacks[2];	\
	int x;	\
	stacks[0] = init_u64list(32);	\
	stacks[1] = init_u64list(32);	\
	push_u64list(stacks[0], 0);	\
	push_u64list(stacks[1], _qsort_n);	\
	x = 0;	\
	while(stacks[0]->size || x > 0){	\
		thread_wait_one(psrt);	\
		if(psrt->divide){	\
			if(psrt->div - psrt->beg <= psrt->end - psrt->div){	\
				push_u64list(stacks[0], psrt->beg);	\
				push_u64list(stacks[1], psrt->div);	\
				push_u64list(stacks[0], psrt->div);	\
				push_u64list(stacks[1], psrt->end);	\
			} else {	\
				push_u64list(stacks[0], psrt->div);	\
				push_u64list(stacks[1], psrt->end);	\
				push_u64list(stacks[0], psrt->beg);	\
				push_u64list(stacks[1], psrt->div);	\
			}	\
			x --;	\
			psrt->divide = 0;	\
		} else if(stacks[0]->size){	\
			psrt->beg = stacks[0]->buffer[--stacks[0]->size];	\
			psrt->end = stacks[1]->buffer[--stacks[1]->size];	\
			psrt->divide = (psrt->end - psrt->beg > _min_blk_size);	\
			if(psrt->divide) x ++;	\
			thread_wake(psrt);	\
		}	\
	}	\
	thread_wait_all(psrt);	\
	thread_beg_close(psrt);	\
	thread_end_close(psrt);	\
	free_u64list(stacks[0]);	\
	free_u64list(stacks[1]);	\
} while(0)

#define quick_median_array(_rs, _rs_size, e_type, expr)	\
({	\
	e_type *_qm_rs, _qm_key;	\
	_qm_rs = (e_type*)(_rs);	\
	size_t _qm_size;	\
	_qm_size = (size_t)(_rs_size);	\
	do {	\
		size_t _qm_i, _qm_j, _qm_beg, _qm_mid, _qm_end;	\
		if(_qm_size == 0){	\
			break;	\
		}	\
		_qm_beg = 0;	\
		_qm_end = _qm_size - 1;	\
		e_type _qm_tmp, a, b;	\
		while(_qm_beg < _qm_end){	\
			_qm_mid = _qm_beg + (_qm_end - _qm_beg) / 2;	\
			a = _qm_rs[_qm_beg]; b = _qm_rs[_qm_mid];	\
			if(expr){ _qm_tmp = _qm_rs[_qm_beg]; _qm_rs[_qm_beg] = _qm_rs[_qm_mid]; _qm_rs[_qm_mid] = _qm_tmp; }	\
			a = _qm_rs[_qm_mid]; b = _qm_rs[_qm_end];	\
			if(expr){	\
				_qm_tmp = _qm_rs[_qm_end]; _qm_rs[_qm_end] = _qm_rs[_qm_mid]; _qm_rs[_qm_mid] = _qm_tmp;	\
				a = _qm_rs[_qm_beg]; b = _qm_rs[_qm_mid];	\
				if(expr){ _qm_tmp = _qm_rs[_qm_beg]; _qm_rs[_qm_beg] = _qm_rs[_qm_mid]; _qm_rs[_qm_mid] = _qm_tmp; }	\
			}	\
			_qm_key = _qm_rs[_qm_mid];	\
			_qm_i = _qm_beg + 1; _qm_j = _qm_end - 1;	\
			while(1){	\
				a = _qm_key;	\
				while(b = _qm_rs[_qm_i], (expr)) _qm_i ++;	\
				b = _qm_key;	\
				while(a = _qm_rs[_qm_j], (expr)) _qm_j --;	\
				if(_qm_i < _qm_j){	\
					_qm_tmp = _qm_rs[_qm_i]; _qm_rs[_qm_i] = _qm_rs[_qm_j]; _qm_rs[_qm_j] = _qm_tmp;	\
					_qm_i ++; _qm_j --;	\
				} else break;	\
			}	\
			if(_qm_i == _qm_j){ _qm_i ++; _qm_j --; }	\
			if(_qm_i <= _qm_size / 2) _qm_beg = _qm_i;	\
			else _qm_end = _qm_j;	\
		}	\
	} while(0);	\
	_qm_rs[_qm_size / 2];	\
})


#define apply_array(rs, rs_size, e_type, expression)	\
do {	\
	size_t _i, _rs_size;	\
	e_type a;	\
	_rs_size = rs_size;	\
	for(_i=0;_i<_rs_size;_i++){	\
		a = (rs)[_i];	\
		expression;	\
	}	\
} while(0)

#define ref_apply_array(rs, rs_size, e_type, expression)	\
do {	\
	size_t _i, _rs_size;	\
	e_type *a;	\
	_rs_size = rs_size;	\
	for(_i=0;_i<_rs_size;_i++){	\
		a = (rs) + _i;	\
		expression;	\
	}	\
} while(0)

#define locate_array(rs, rs_size, e_type, expr)		\
({	\
	 size_t _i, _size;	\
	 e_type a;	\
	 _size = rs_size;	\
	 for(_i=0;_i<_size;_i++){	\
		 a = rs[_i];	\
		 if(expr) break;	\
	 };	\
	 _i;	\
 })

// sort the array according to bool value (true then flase), and return the size of trues
#define apply_xchg_array(rs, rs_size, e_type, expr)	\
({	\
	size_t _i, _j, _size;	\
	e_type a;	\
	_size = rs_size;	\
	for(_i=_j=0;_i<_size;_i++){	\
		a = (rs)[_i];	\
		if(!(expr)) continue;	\
		if(_j < _i){	\
			a = (rs)[_j];	\
			(rs)[_j] = (rs)[_i];	\
			(rs)[_i] = a;	\
		}	\
		_j ++;	\
	}	\
	_j;	\
})

#define ref_filter_array(rs, rs_size, e_type, whether_filter_expr)	\
({	\
	size_t _i, _j, _size;	\
	e_type *_rs, *a, *b, *c, ELE_NULL;	\
	_rs = (e_type*)(rs);	\
	_size = rs_size;	\
	b = c = &ELE_NULL;	\
	memset(b, 0xFF, sizeof(e_type));	\
	for(_i=_j=0;_i<_size;_i++,b=a){	\
		a = _rs + _i;	\
		if(whether_filter_expr) continue; \
		if(_j < _i){	\
			_rs[_j] = _rs[_i];	\
		}	\
		c = a;	\
		_j ++;	\
	}	\
	_j;	\
})

#define incfill_array(rs, rs_size, e_type, begval)	\
({	\
	e_type *_rs, _val;	\
	size_t _i, _sz;	\
	_rs = (e_type*)(rs);	\
	_sz = (size_t)(rs_size);	\
	_val = (e_type)(begval);	\
	for(_i=0;_i<_sz;_i++){	\
		_rs[_i] = _val ++;	\
	}	\
	_val;	\
})

#define define_quick_sort(name, e_type, is_greater_func)	\
static inline void name(e_type *rs, size_t n, void *obj){	\
	size_t s, e, i, j, m, stack[64][2], x;	\
	e_type p, t;	\
	if(n < 2) return;	\
	x = 0;	\
	stack[x][0] = 0; stack[x][1] = n - 1; x ++;	\
	while(x){	\
		x --; s = stack[x][0]; e = stack[x][1];	\
		m = s + (e - s) / 2;	\
		if(is_greater_func(rs[s], rs[m], obj) > 0){ t = rs[s]; rs[s] = rs[m]; rs[m] = t; }	\
		if(is_greater_func(rs[m], rs[e], obj) > 0){	\
			t = rs[e]; rs[e] = rs[m]; rs[m] = t;	\
			if(is_greater_func(rs[s], rs[m], obj) > 0){ t = rs[s]; rs[s] = rs[m]; rs[m] = t; }	\
		}	\
		p = rs[m];	\
		i = s + 1; j = e - 1;	\
		while(1){	\
			while(is_greater_func(p, rs[i], obj) > 0) i ++;	\
			while(is_greater_func(rs[j], p, obj) > 0) j --;	\
			if(i < j){	\
				t = rs[i]; rs[i] = rs[j]; rs[j] = t;	\
				i ++; j --;	\
			} else break;	\
		}	\
		if(i == j){ i ++; j --; }	\
		if(j - s > e - i){	\
			if(s + 4 < j){ stack[x][0] = s; stack[x][1] = j; x ++; }	\
			if(i + 4 < e){ stack[x][0] = i; stack[x][1] = e; x ++; }	\
		} else {	\
			if(i + 4 < e){ stack[x][0] = i; stack[x][1] = e; x ++; }	\
			if(s + 4 < j){ stack[x][0] = s; stack[x][1] = j; x ++; }	\
		}	\
	}	\
	for(i=0;i<n;i++){	\
		x = 0;	\
		for(j=n-1;j>i;j--){	\
			if(is_greater_func(rs[j - 1], rs[j], obj) > 0){ t = rs[j - 1]; rs[j - 1] = rs[j]; rs[j] = t; x = 1; }	\
		}	\
		if(x == 0) break;	\
	}	\
	if(obj == obj) return;	\
}

#define indexmerge_array(_size1, _size2, cmp_expr, proc_eq_expr, proc_lt_expr, proc_gt_expr)	\
{	\
	size_t a, b, __mcnt[2];	\
	int __mret;	\
	__mcnt[0] = _size1;	\
	__mcnt[1] = _size2;	\
	a = b = 0;	\
	while(a < __mcnt[0] && b < __mcnt[1]){	\
		__mret = (cmp_expr);	\
		if(__mret == 0){	\
			proc_eq_expr;	\
			a ++;	\
			b ++;	\
		} else if(__mret < 0){	\
			proc_lt_expr;	\
			a ++;	\
		} else {	\
			proc_gt_expr;	\
			b ++;	\
		}	\
	}	\
	while(a < __mcnt[0]){	\
		proc_lt_expr;	\
		a ++;	\
	}	\
	while(b < __mcnt[1]){	\
		proc_gt_expr;	\
		b ++;	\
	}	\
}

#define reverse_array(_rs, _rs_size, e_type)	\
do {	\
	size_t i, n;	\
	n = (_rs_size);	\
	e_type* __rs = (_rs);	\
	e_type e;	\
	for(i=0;i<n>>1;i++){	\
		e = __rs[i]; __rs[i] = __rs[n-1-i]; __rs[n-1-i] = e;	\
	}	\
} while(0)

#define define_reverse_array(name, e_type)	\
static inline void name(e_type *list, size_t size){	\
	size_t i, j;	\
	e_type t;	\
	if(size == 0) return;	\
	i = 0;	\
	j = size - 1;	\
	while(i < j){	\
		t = list[i]; list[i] = list[j]; list[j] = t;	\
		i ++; j --;	\
	}	\
}

#define define_apply_array(name, e_type, apply_func)	\
static inline size_t name(e_type *list, size_t size, void *ref){	\
	size_t i, ret;	\
	ret = 0;	\
	for(i=0;i<size;i++){	\
		ret += apply_func(list[i], ref);	\
	}	\
	return ret;	\
	ref = NULL;	\
}

#define define_search_array(name, e_type, cmp_func)	\
static inline long long name(e_type *array, long long size, e_type key, void *ref){	\
	long long i, j, m;	\
	i = 0;	\
	j = size;	\
	while(i < j){	\
		m = i + (j - i) / 2;	\
		if(cmp_func(array[m], key, ref) < 0){	\
			i = m + 1;	\
		} else {	\
			j = m;	\
		}	\
	}	\
	if(i < size && cmp_func(array[i], key, ref) == 0) return i;	\
	else return - (i + 1);	\
	if(ref) return 0;	\
}

#define bsearch_array(_rs_array, _rs_size, e_type, _bs_ret, is_a_less_than_your)	\
	\
do {	\
	e_type*_bs_rs;	\
	e_type a;	\
	_bs_rs = (e_type*)(_rs_array);	\
	size_t _bs_size;	\
	_bs_size = _rs_size;	\
	size_t _bs_i, _bs_j, _bs_m;	\
	_bs_i = 0;	\
	_bs_j = _bs_size;	\
	while(_bs_i < _bs_j){	\
		_bs_m = _bs_i + (_bs_j - _bs_i) / 2;	\
		a = _bs_rs[_bs_m];	\
		if(is_a_less_than_your){	\
			_bs_i = _bs_m + 1;	\
		} else {	\
			_bs_j = _bs_m;	\
		}	\
	}	\
	_bs_ret = _bs_i;	\
} while(0)

#define shuffle_array(dat_array, ord_array, rev_array, dat_size)	\
do {	\
	typeof((dat_array)[0]) _shf_e;	\
	size_t _shf_i, _shf_j, _shf_t, _shf_n;	\
	_shf_n = dat_size;	\
	for(_shf_i=0;_shf_i<_shf_n;_shf_i++){	\
		(rev_array)[(ord_array)[_shf_i]] = _shf_i;	\
	}	\
	for(_shf_i=0;_shf_i<_shf_n;_shf_i++){	\
		_shf_j = (rev_array)[_shf_i];	\
		while(_shf_j != _shf_i){	\
			_shf_e = (dat_array)[_shf_j]; (dat_array)[_shf_j] = (dat_array)[_shf_i]; (dat_array)[_shf_i] = _shf_e;	\
			_shf_t = (rev_array)[_shf_j]; (rev_array)[_shf_j] = _shf_j; _shf_j = _shf_t;	\
		}	\
	}	\
} while(0)

#define shuffle_bytes_array(dat_array, ord_array, rev_array, dat_size, ele_len)	\
do {	\
	size_t _shf_i, _shf_j, _shf_t, _shf_n;	\
	_shf_n = dat_size;	\
	for(_shf_i=0;_shf_i<_shf_n;_shf_i++){	\
		(rev_array)[(ord_array)[_shf_i]] = _shf_i;	\
	}	\
	for(_shf_i=0;_shf_i<_shf_n;_shf_i++){	\
		_shf_j = (rev_array)[_shf_i];	\
		while(_shf_j != _shf_i){	\
			swap_bytes(((void*)(dat_array)) + ele_len * _shf_i, ((void*)(dat_array)) + ele_len * _shf_j, ele_len);	\
			_shf_t = (rev_array)[_shf_j]; (rev_array)[_shf_j] = _shf_j; _shf_j = _shf_t;	\
		}	\
	}	\
} while(0)

#endif
