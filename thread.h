#ifndef __THEAD_RJ_H
#define __THEAD_RJ_H

#include <pthread.h>
#include <assert.h>
#include <stdlib.h>

#define lock_cmpxchg(location, value, comparand)    \
({	\
__typeof (*location) _result;	\
if(sizeof(*location) == 1){	\
	__asm__ __volatile__ (	\
		"lock\n\t"	\
		"cmpxchgb %b2,(%1)"	\
		:"=a" (_result)	\
		:"r"  (location), "r" (value), "a" (comparand)	\
		:"memory", "cc");	\
} else if(sizeof(*location) == 2){	\
	__asm__ __volatile__ (	\
		"lock\n\t"	\
		"cmpxchgw %w2,(%1)"	\
		:"=a" (_result)	\
		:"r"  (location), "r" (value), "a" (comparand)	\
		:"memory", "cc");	\
} else if(sizeof(*location) == 4){	\
	__asm__ __volatile__ (	\
		"lock\n\t"	\
		"cmpxchgl %w2,(%1)"	\
		:"=a" (_result)	\
		:"r"  (location), "r" (value), "a" (comparand)	\
		:"memory", "cc");	\
} else {	\
	__asm__ __volatile__ (	\
		"lock\n\t"	\
		"cmpxchgq %2,(%1)"	\
		:"=a" (_result)	\
		:"r"  (location), "r" (value), "a" (comparand)	\
		:"memory", "cc");	\
}	\
_result;	\
})

#define THREAD_STATE_IDLE	0
#define THREAD_STATE_BUSY	1
#define THREAD_STATE_DONE	2
#define THREAD_STATE_UAVL	3

#define thread_typeof(tname) (struct tname##_struct)

#define thread_begin_def(tname)	\
struct tname##_struct {		\
	struct tname##_struct *tname##_params;	\
	struct tname##_struct *tname##_array;	\
	int n_cpu;	\
	int t_idx;	\
	volatile int running;	\
	volatile int state;	\
	volatile int once;	\
	pthread_mutex_t *mutex_lock;	\
	pthread_rwlock_t *rw_lock;	\
	pthread_mutex_t _COND_LOCK;	\
	pthread_cond_t _COND
#define thread_end_def(tname) }

#define thread_def_shared_vars(tname)	\
	struct tname##_struct *tname##_params;	\
	pthread_t *tname##_pids;	\
	pthread_mutex_t *tname##_mlock;	\
	pthread_rwlock_t *tname##_rwlock;	\
	struct tname##_struct *tname;	\
	int tname##_i;	\
	int tname##_j;	\
	int tname##_var_next

#define thread_beg_def(tname) thread_begin_def(tname)

#define thread_begin_func_core(tname) inline void* thread_##tname##_func(void *obj){\
	struct tname##_struct * tname = (struct tname##_struct *)obj;\
	int tname##_var_i;\
	struct tname##_struct * tname##_params;	\
	tname##_params = tname->tname##_params;	\
	if(tname##_params + tname->t_idx != tname){	\
		fprintf(stderr, " --  Unexcepted error in thread [%s] in %s -- %s:%d --\n", #tname, __FUNCTION__, __FILE__, __LINE__);	\
	}
#define thread_begin_func(tname) static thread_begin_func_core(tname)
#define thread_beg_func(tname) thread_begin_func(tname)
#define thread_beg_func_inline(tname) inline void* thread_##tname##_func(void *obj){\
	struct tname##_struct * tname = (struct tname##_struct *)obj;\
	int tname##_var_i;\
	struct tname##_struct * tname##_params;	\
	tname##_params = tname->tname##_params;	\
	if(tname##_params + tname->t_idx != tname){	\
		fprintf(stderr, " --  Unexcepted error in thread [%s] in %s -- %s:%d --\n", #tname, __FUNCTION__, __FILE__, __LINE__);	\
	}
#define thread_begin_loop(tname)	\
	pthread_mutex_lock(&tname->_COND_LOCK);	\
	tname->state = THREAD_STATE_IDLE;	\
	pthread_cond_signal(&tname->_COND);	\
	pthread_mutex_unlock(&tname->_COND_LOCK);	\
	while(tname->running){	\
		if(tname->state != THREAD_STATE_BUSY){	\
			struct timespec _timeout;	\
			pthread_mutex_lock(&tname->_COND_LOCK);	\
			clock_gettime(CLOCK_REALTIME, &_timeout);	\
			_timeout.tv_nsec += 1000000;	\
			pthread_cond_timedwait(&tname->_COND, &tname->_COND_LOCK, &_timeout);	\
			pthread_mutex_unlock(&tname->_COND_LOCK);	\
			continue;	\
		}	\
		for(tname##_var_i=0;tname##_var_i<1;tname##_var_i++){
#define thread_beg_loop(tname) thread_begin_loop(tname)
#define thread_begin_syn(tname) pthread_mutex_lock(tname->mutex_lock)
#define thread_beg_syn(tname) thread_begin_syn(tname)
#define thread_end_syn(tname) pthread_mutex_unlock(tname->mutex_lock)
#define thread_beg_syn_read(tname) pthread_rwlock_rdlock(tname->rw_lock)
#define thread_end_syn_read(tname) pthread_rwlock_unlock(tname->rw_lock)
#define thread_beg_syn_write(tname) pthread_rwlock_wrlock(tname->rw_lock)
#define thread_end_syn_write(tname) pthread_rwlock_unlock(tname->rw_lock)
#define thread_end_loop(tname)	\
		}	\
		if(tname->once){	\
			pthread_mutex_lock(&tname->_COND_LOCK);	\
			tname->state = THREAD_STATE_DONE;	\
			pthread_cond_signal(&tname->_COND);	\
			pthread_mutex_unlock(&tname->_COND_LOCK);	\
		}	\
	}	\
	pthread_mutex_lock(&tname->_COND_LOCK);	\
	tname->state = THREAD_STATE_DONE;	\
	pthread_cond_signal(&tname->_COND);	\
	pthread_mutex_unlock(&tname->_COND_LOCK)
#define thread_end_func(tname) return NULL; }

#define thread_preprocess(tname)		\
	thread_def_shared_vars(tname);	\
	(void)(tname##_params);	\
	(void)(tname##_pids);	\
	(void)(tname##_mlock);	\
	(void)(tname##_rwlock);	\
	(void)(tname);	\
	tname##_i = 0;	\
	tname##_j = 0;	\
	tname##_var_next = 0

#define thread_prepare(tname) thread_preprocess(tname)
#define thread_begin_init(tname, n_thread) assert(n_thread > 0);\
	tname##_params = (struct tname##_struct *)malloc(sizeof(struct tname##_struct) * n_thread);\
	tname##_pids = (pthread_t *)malloc(sizeof(pthread_t) * n_thread); \
	tname##_mlock = calloc(1, sizeof(pthread_mutex_t));	\
	*tname##_mlock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;	\
	tname##_rwlock = calloc(1, sizeof(pthread_rwlock_t));	\
	*tname##_rwlock = (pthread_rwlock_t)PTHREAD_RWLOCK_INITIALIZER;	\
	for(tname##_i=0,tname##_j=0;tname##_i<(int)(n_thread);tname##_i++){ \
		tname = tname##_params + tname##_i;\
		tname->mutex_lock = tname##_mlock;\
		tname->rw_lock = tname##_rwlock;\
		tname->_COND_LOCK = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;	\
		tname->_COND = (pthread_cond_t)PTHREAD_COND_INITIALIZER;	\
		tname->n_cpu      = n_thread;\
		tname->t_idx      = tname##_i;\
		tname->running    = 1;\
		tname->state      = THREAD_STATE_UAVL;\
		tname->once       = 1;\
		tname->tname##_params = (struct tname##_struct*)tname##_params;\
		tname->tname##_array  = (struct tname##_struct*)tname##_params
#define thread_beg_init(tname, n_thread) thread_begin_init(tname, n_thread)

#define thread_end_init(tname) \
		if(pthread_create(tname##_pids + tname##_i, NULL, thread_##tname##_func, (void*)tname) != 0){\
			fprintf(stderr, " -- Failed to create thread [%s, %04d] in %s -- %s:%d --\n", #tname, tname##_i, __FUNCTION__, __FILE__, __LINE__);\
			exit(1);\
		}\
		while(1){	\
			int _stop;	\
			_stop = 0;	\
			if(tname->state == THREAD_STATE_IDLE) _stop = 1;	\
			else {	\
				struct timespec _timeout;	\
				pthread_mutex_lock(&tname->_COND_LOCK);	\
				clock_gettime(CLOCK_REALTIME, &_timeout);	\
				_timeout.tv_nsec += 1000000;	\
				pthread_cond_timedwait(&tname->_COND, &tname->_COND_LOCK, &_timeout);	\
				if(tname->state == THREAD_STATE_IDLE) _stop = 1;	\
				pthread_mutex_unlock(&tname->_COND_LOCK);	\
			}	\
			if(_stop) break;	\
		}	\
	}\
	tname = tname##_params + 0;	\
	tname##_var_next = 0;	\
	tname##_i = 0;	\
	tname##_j = 0

#define thread_export_core(tname, _tname, params, pids, mlock, rwlock, i, j, next)	\
do {	\
	_tname = tname;	\
	params = tname##_params;	\
	pids   = tname##_pids;	\
	mlock  = tname##_mlock;	\
	rwlock = tname##_rwlock;	\
	i      = tname##_i;	\
	j      = tname##_j;	\
	next   = tname##_var_next;	\
} while(0)

#define thread_export(tname, obj) thread_export_core(tname, (obj)->tname, (obj)->tname##_params, (obj)->tname##_pids, (obj)->tname##_mlock, (obj)->tname##_rwlock, (obj)->tname##_i, (obj)->tname##_j, (obj)->tname##_var_next)

#define thread_import_core(tname, _tname, params, pids, mlock, rwlock, i, j, next)	\
do {	\
	tname          = _tname;	\
	tname##_params = params;	\
	tname##_pids   = pids;	\
	tname##_mlock  = mlock;	\
	tname##_rwlock = rwlock;	\
	tname##_i      = i;	\
	tname##_j      = j;	\
	tname##_var_next   = next;	\
} while(0)

#define thread_import(tname, obj) thread_import_core(tname, (obj)->tname, (obj)->tname##_params, (obj)->tname##_pids, (obj)->tname##_mlock, (obj)->tname##_rwlock, (obj)->tname##_i, (obj)->tname##_j, (obj)->tname##_var_next)

#define thread_begin_operate(tname, idx) tname = tname##_params + idx
#define thread_beg_operate(tname, idx) thread_begin_operate(tname, idx)
#define thread_wake(tname)  do { pthread_mutex_lock(&(tname)->_COND_LOCK); (tname)->state = THREAD_STATE_BUSY; pthread_cond_signal(&(tname)->_COND); pthread_mutex_unlock(&(tname)->_COND_LOCK); } while(0)
#define thread_wake_all(tname)  do { thread_beg_iter(tname); thread_wake(tname); thread_end_iter(tname); } while(0);
#define thread_is_idle(tname) ((tname)->state == THREAD_STATE_IDLE || (tname)->state == THREAD_STATE_DONE)
#define thread_is_done(tname) ((tname)->state == THREAD_STATE_DONE)
#define thread_wait(tname)	\
	while(1){	\
		int _stop;	\
		_stop = 0;	\
		pthread_mutex_lock(&tname->_COND_LOCK);	\
		if(thread_is_idle(tname)) _stop = 1;	\
		else {	\
			struct timespec _timeout;	\
			clock_gettime(CLOCK_REALTIME, &_timeout);	\
			_timeout.tv_nsec += 1000;	\
			pthread_cond_timedwait(&tname->_COND, &tname->_COND_LOCK, &_timeout);	\
			if(thread_is_idle(tname)) _stop = 1;	\
		}	\
		pthread_mutex_unlock(&tname->_COND_LOCK);	\
		if(_stop) break;	\
	}	\
	tname->state = THREAD_STATE_IDLE
#define thread_wait_next(tname) do { thread_beg_operate(tname, tname##_var_next); thread_wait(tname); tname##_var_next = (tname##_var_next + 1) % tname##_params[0].n_cpu; } while(0)
#define thread_end_operate(tname, idx)   tname = NULL
#define thread_begin_iter(tname) { struct tname##_struct * tname = NULL; int tname##_i; for(tname##_i=0;tname##_i<tname##_params[0].n_cpu;tname##_i++){ tname = tname##_params + tname##_i
#define thread_beg_iter(tname) thread_begin_iter(tname)
#define thread_ncpus(tname) (tname->n_cpu)
#define thread_index(tname) (tname->t_idx)
#define thread_end_iter(tname) } }
#define thread_access(tname, idx) (tname##_params + idx)

#define thread_beg_monitor(tname, usec)	\
while(1){	\
	nano_sleep(usec)

#define thread_end_monitor(tname)	\
	for(tname##_j=0;tname##_j<tname##_params[0].n_cpu;tname##_j++){	\
		if(tname##_params[tname##_j].state == THREAD_STATE_BUSY) break;	\
	}	\
	if(tname##_j==tname##_params[0].n_cpu) break;	\
}

#define thread_wait_one(tname)	\
({	\
	int ret;	\
	while(1){	\
		for(;tname##_j<tname##_params[0].n_cpu;tname##_j++){	\
			if(tname##_params[tname##_j].state != THREAD_STATE_BUSY){	\
				tname = tname##_params + tname##_j;	\
				break;	\
			}	\
		}	\
		if(tname##_j >= tname##_params[0].n_cpu){	\
			tname##_j = 0;	\
			nano_sleep(10);	\
		} else {	\
			tname##_j = (tname##_j + 1) % tname##_params[0].n_cpu;	\
			break;	\
		}	\
	}	\
	ret = tname->state;	\
	tname->state = THREAD_STATE_IDLE;	\
	ret;	\
})

#define thread_wait_done(tname)	\
({	\
	int ret;	\
	while(1){	\
		int _nrun_;	\
		_nrun_ = 0;	\
		for(tname##_j=0;tname##_j<tname##_params[0].n_cpu;tname##_j++){	\
			if(tname##_params[tname##_j].state == THREAD_STATE_DONE){	\
				tname = tname##_params + tname##_j;	\
				break;	\
			} else if(tname##_params[tname##_j].state == THREAD_STATE_BUSY){	\
				_nrun_ ++;	\
			}	\
		}	\
		if(tname##_j >= tname##_params[0].n_cpu){	\
			tname##_j = 0;	\
			if(_nrun_ == 0){	\
				tname = tname##_params + tname##_j;	\
				tname##_j = (tname##_j + 1) % tname##_params[0].n_cpu;	\
				break;	\
			} else {	\
				nano_sleep(10);	\
			}	\
		} else {	\
			tname##_j = (tname##_j + 1) % tname##_params[0].n_cpu;	\
			break;	\
		}	\
	}	\
	ret = tname->state;	\
	tname->state = THREAD_STATE_IDLE;	\
	ret;	\
})

#define thread_test_all(tname, expr) ({int ret = 1; thread_beg_iter(tname); if(!(expr)){ ret = 0; break;} thread_end_iter(tname); ret;})

#define thread_count_all(tname, expr) ({int ret = 0; thread_beg_iter(tname); if((expr)){ ret ++; } thread_end_iter(tname); ret;})

#define thread_all_idle(tname) thread_test_all(tname, (tname)->state != THREAD_STATE_BUSY)

#define thread_wait_all(tname) { thread_begin_iter(tname); thread_wait(tname); thread_end_iter(tname); }

#define thread_apply_all(tname, expr) { thread_begin_iter(tname); (expr); thread_wake(tname); thread_end_iter(tname); thread_wait_all(tname); }

#define thread_begin_close(tname) for(tname##_i=0;tname##_i<tname##_params[0].n_cpu;tname##_i++){ \
		tname = tname##_params + tname##_i;\
		thread_wait(tname);\
		pthread_mutex_lock(&tname->_COND_LOCK);	\
		tname->running = 0; \
		pthread_cond_signal(&tname->_COND);	\
		pthread_mutex_unlock(&tname->_COND_LOCK);	\
		pthread_join(tname##_pids[tname##_i], NULL)
#define thread_beg_close(tname) thread_begin_close(tname)
#define thread_end_close(tname) }  free((void*)tname##_params); free(tname##_pids); free(tname##_mlock); free(tname##_rwlock)

#define thread_run(tname, ncpu, vars_expr, init_expr, free_expr, pre_expr, loop_expr, post_expr, invoke_expr)	\
{	\
	thread_beg_def(tname);	\
	vars_expr	\
	thread_end_def(tname);	\
	thread_begin_func_core(tname);	\
	pre_expr	\
	thread_beg_loop(tname);	\
	loop_expr	\
	thread_end_loop(tname);	\
	post_expr	\
	thread_end_func(tname);	\
	{	\
		thread_preprocess(tname);	\
		thread_beg_init(tname, ncpu);	\
		init_expr	\
		thread_end_init(tname);	\
		invoke_expr	\
		thread_beg_close(tname);	\
		free_expr	\
		thread_end_close(tname);	\
	}	\
}

#define THREAD_EXPR(...) __VA_ARGS__

#define thread_fast_run(tname, ncpu, loop_expr) thread_run(tname, ncpu, , , , THREAD_EXPR(int NCPU; int TIDX; NCPU = tname->n_cpu; TIDX = tname->t_idx;), THREAD_EXPR(loop_expr;), , THREAD_EXPR(thread_wake_all(tname); thread_wait_all(tname);))

#define thread_fast_run2(tname, ncpu, expr)	\
{	\
	thread_beg_def(tname);	\
	thread_end_def(tname);	\
	thread_begin_func_core(tname);	\
	int NCPU, TIDX;	\
	NCPU = tname->n_cpu;	\
	TIDX = tname->t_idx;	\
	UNUSED(NCPU);	\
	UNUSED(TIDX);	\
	thread_beg_loop(tname);	\
	(expr);	\
	thread_end_loop(tname);	\
	thread_end_func(tname);	\
	{	\
		thread_preprocess(tname);	\
		thread_beg_init(tname, ncpu);	\
		thread_end_init(tname);	\
		thread_wake_all(tname);	\
		thread_wait_all(tname);	\
		thread_beg_close(tname);	\
		thread_end_close(tname);	\
	}	\
}

#endif
