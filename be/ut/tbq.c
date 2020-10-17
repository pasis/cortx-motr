/* -*- C -*- */
/*
 * Copyright (c) 2015-2020 Seagate Technology LLC and/or its Affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For any questions about this software or licensing,
 * please email opensource@seagate.com or cortx-questions@seagate.com.
 *
 */



/**
 * @addtogroup be
 *
 * @{
 */

#define M0_TRACE_SUBSYSTEM M0_TRACE_SUBSYS_BE
#include "lib/trace.h"

#include "be/tbq.h"

#include "lib/memory.h"         /* M0_ALLOC_PTR */
#include "lib/semaphore.h"      /* m0_semaphore */
#include "lib/atomic.h"         /* m0_atomic64 */
#include "lib/arith.h"          /* m0_rnd64 */
#include "lib/misc.h"           /* m0_reduce */
#include "lib/buf.h"            /* m0_buf_eq */

#include "ut/threads.h"         /* m0_be_theads_start */
#include "ut/ut.h"              /* M0_UT_ASSERT */

#include "be/tx_credit.h"       /* M0_BE_TX_CREDIT */
#include "be/op.h"              /* M0_BE_OP_SYNC */


enum be_ut_tbq_test {
	BE_UT_TBQ_1_1_1,
	BE_UT_TBQ_100_1_1,
	BE_UT_TBQ_100_1_10,
	BE_UT_TBQ_100_10_1,
	BE_UT_TBQ_100_10_10,
	BE_UT_TBQ_10_100_1,
	BE_UT_TBQ_10_100_5,
	BE_UT_TBQ_10_1_100,
	BE_UT_TBQ_10_5_100,
	BE_UT_TBQ_10_100_100,
	BE_UT_TBQ_NR,
};

struct be_ut_tbq_cfg {
	uint32_t butc_q_size_max;
	uint32_t butc_producers;
	uint32_t butc_consumers;
	uint64_t butc_items_nr;
};

struct be_ut_tbq_result {
	uint64_t butr_before;
	uint64_t butr_data_index;
};

struct be_ut_tbq_ctx {
	struct be_ut_tbq_cfg    *butx_cfg;
	struct m0_be_tbq        *butx_bbq;
	/* producer increments and takes butx_data[] with the index returned */
	struct m0_atomic64       butx_pos_in;
	/*
	 * consumers increments and stores to butx_out[] with the index returned
	 */
	struct m0_atomic64       butx_pos_out;
	struct m0_atomic64       butx_before;
	struct m0_atomic64       butx_after;
	struct m0_be_tbq_data   *butx_data;
	struct be_ut_tbq_result *butx_result;
};

struct be_ut_tbq_thread_param {
	struct be_ut_tbq_ctx *butqp_ctx;
	/*
	 * Start barrier to launch all threads as close to each other as
	 * possible.
	 */
	struct m0_semaphore   butqp_sem_start;
	bool                  butqp_is_producer;
	/*
	 * Thread index, starts from 0 for producers and starts from 0 for
	 * consumers.
	 */
	uint64_t              butqp_index;
	/* Number of items to put/get to/from the queue. */
	uint64_t              butqp_items_nr;
	uint64_t              butqp_peeks_successful;
	uint64_t              butqp_peeks_unsuccessful;
};

#define BE_UT_TBQ_TEST(q_size_max, producers, consumers, items_nr)      \
{                                                                       \
	.butc_q_size_max = (q_size_max),                                \
	.butc_producers  = (producers),                                 \
	.butc_consumers  = (consumers),                                 \
	.butc_items_nr   = (items_nr)                                   \
}

static struct be_ut_tbq_cfg be_ut_tbq_tests_cfg[BE_UT_TBQ_NR] = {
	[BE_UT_TBQ_1_1_1]      = BE_UT_TBQ_TEST(  1,   1,   1,  100),
	[BE_UT_TBQ_100_1_1]    = BE_UT_TBQ_TEST(100,   1,   1, 1000),
	[BE_UT_TBQ_100_1_10]   = BE_UT_TBQ_TEST(100,   1,  10, 1000),
	[BE_UT_TBQ_100_10_1]   = BE_UT_TBQ_TEST(100,  10,   1, 1000),
	[BE_UT_TBQ_100_10_10]  = BE_UT_TBQ_TEST(100,  10,  10, 1000),
	[BE_UT_TBQ_10_100_1]   = BE_UT_TBQ_TEST( 10, 100,   1, 1000),
	[BE_UT_TBQ_10_100_5]   = BE_UT_TBQ_TEST( 10, 100,   5, 1000),
	[BE_UT_TBQ_10_1_100]   = BE_UT_TBQ_TEST( 10,   1, 100, 1000),
	[BE_UT_TBQ_10_5_100]   = BE_UT_TBQ_TEST( 10,   5, 100, 1000),
	[BE_UT_TBQ_10_100_100] = BE_UT_TBQ_TEST( 10, 100, 100, 1000),
};

#undef BE_UT_TBQ_TEST

static uint64_t be_ut_tbq_data_index(struct be_ut_tbq_ctx  *ctx,
                                     struct m0_be_tbq_data *data)
{
	return (struct m0_be_tbq_data *)data->bbd_user - ctx->butx_data;
}

static void be_ut_tbq_try_peek(struct be_ut_tbq_thread_param *param,
                               struct be_ut_tbq_ctx          *ctx)
{
	struct m0_be_tbq_data data;
	struct m0_buf         buf;
	bool                  result;

	result = m0_be_tbq_peek(ctx->butx_bbq, &data);
	if (result) {
		++param->butqp_peeks_successful;
		buf = M0_BUF_INIT_PTR(&ctx->butx_data[
		                      be_ut_tbq_data_index(ctx, &data)]);
		M0_UT_ASSERT(m0_buf_eq(&M0_BUF_INIT_PTR(&data), &buf));
	} else {
		++param->butqp_peeks_unsuccessful;
	}
}

static void be_ut_tbq_thread(void *_param)
{
	struct m0_be_tbq_data          data;
	struct be_ut_tbq_thread_param *param = _param;
	struct be_ut_tbq_ctx          *ctx = param->butqp_ctx;
	struct m0_be_tbq              *bbq = ctx->butx_bbq;
	struct m0_be_op               *op;
	uint64_t                       i;
	uint64_t                       index;
	uint64_t                       before;

	M0_ALLOC_PTR(op);
	M0_UT_ASSERT(op != NULL);
	m0_be_op_init(op);
	m0_semaphore_down(&param->butqp_sem_start);
	for (i = 0; i < param->butqp_items_nr; ++i) {
		if (param->butqp_is_producer) {
			index = m0_atomic64_add_return(&ctx->butx_pos_in,
						       1) - 1;
			m0_be_tbq_lock(bbq);
			be_ut_tbq_try_peek(param, ctx);
			m0_be_tbq_put(bbq, op, &ctx->butx_data[index]);
			m0_be_tbq_unlock(bbq);
			m0_be_op_wait(op);
		} else {
			before = m0_atomic64_add_return(&ctx->butx_before,
							1) - 1;
			m0_be_tbq_lock(bbq);
			m0_be_tbq_get(bbq, op, &data);
			be_ut_tbq_try_peek(param, ctx);
			m0_be_tbq_unlock(bbq);
			m0_be_op_wait(op);
			index = m0_atomic64_add_return(&ctx->butx_pos_out,
						       1) - 1;
			ctx->butx_result[index] = (struct be_ut_tbq_result){
				.butr_before     = before,
				.butr_data_index =
					be_ut_tbq_data_index(ctx, &data),
			};
		}
		m0_be_op_reset(op);
	}
	m0_be_op_fini(op);
	m0_free(op);
}

static void be_ut_tbq(enum be_ut_tbq_test test)
{
	struct m0_ut_threads_descr    *td;
	struct be_ut_tbq_thread_param *params;
	struct be_ut_tbq_cfg          *test_cfg = &be_ut_tbq_tests_cfg[test];
	struct m0_be_tbq_cfg           bbq_cfg = {
		.bqc_q_size_max       = test_cfg->butc_q_size_max,
		.bqc_producers_nr_max = test_cfg->butc_producers,
		.bqc_consumers_nr_max = test_cfg->butc_consumers,
	};
	struct be_ut_tbq_ctx          *ctx;
	struct m0_be_tbq              *bbq;
	uint32_t                       threads_nr;
	uint32_t                       items_nr = test_cfg->butc_items_nr;
	uint64_t                       i;
	uint64_t                       seed = test;
	uint32_t                       divisor;
	uint32_t                       remainder;
	int                            rc;

	M0_ALLOC_PTR(bbq);
	M0_ASSERT(bbq != NULL);
	rc = m0_be_tbq_init(bbq, &bbq_cfg);
	M0_ASSERT_INFO(rc == 0, "rc=%d", rc);

	M0_ALLOC_PTR(ctx);
	M0_UT_ASSERT(ctx != NULL);
	ctx->butx_cfg = test_cfg;
	ctx->butx_bbq = bbq;
	m0_atomic64_set(&ctx->butx_pos_in, 0);
	m0_atomic64_set(&ctx->butx_pos_out, 0);
	M0_ALLOC_ARR(ctx->butx_data, items_nr);
	M0_UT_ASSERT(ctx->butx_data != NULL);
	M0_ALLOC_ARR(ctx->butx_result, items_nr);
	M0_UT_ASSERT(ctx->butx_result != NULL);
	for (i = 0; i < items_nr; ++i) {
		ctx->butx_data[i] = (struct m0_be_tbq_data){
			.bbd_user = &ctx->butx_data[i],
			.bbd_credit =
				M0_BE_TX_CREDIT(m0_rnd64(&seed) % 0x100 + 1,
				                m0_rnd64(&seed) % 0x100 + 1),
			.bbd_payload_size = m0_rnd64(&seed) % 0x1000 + 1,
		};
	}
	threads_nr = test_cfg->butc_producers + test_cfg->butc_consumers;
	M0_ALLOC_ARR(params, threads_nr);
	for (i = 0; i < threads_nr; ++i) {
		params[i].butqp_ctx = ctx;
		m0_semaphore_init(&params[i].butqp_sem_start, 0);
		params[i].butqp_is_producer = i < test_cfg->butc_producers;
		if (params[i].butqp_is_producer) {
			params[i].butqp_index = i;
			divisor = test_cfg->butc_producers;
		} else {
			params[i].butqp_index = i - test_cfg->butc_producers;
			divisor = test_cfg->butc_consumers;
		}
		remainder = items_nr % divisor;
		params[i].butqp_items_nr = items_nr / divisor +
			(remainder == 0 ?
			 0 : remainder < params[i].butqp_index);
	}
	M0_UT_ASSERT(m0_reduce(j, test_cfg->butc_producers,
			       0, + params[j].butqp_items_nr) == items_nr);
	M0_UT_ASSERT(m0_reduce(j, test_cfg->butc_consumers,
			       0, + params[test_cfg->butc_producers +
					   j].butqp_items_nr) == items_nr);

	M0_ALLOC_PTR(td);
	M0_UT_ASSERT(td != NULL);
	td->utd_thread_func = &be_ut_tbq_thread;
	m0_ut_threads_start(td, threads_nr, params, sizeof(params[0]));
	for (i = 0; i < threads_nr; ++i)
		m0_semaphore_up(&params[i].butqp_sem_start);
	/* work is done sometime around here */
	m0_ut_threads_stop(td);
	m0_free(td);

	for (i = 0; i < threads_nr; ++i)
		m0_semaphore_fini(&params[i].butqp_sem_start);
	m0_free(params);
	m0_free(ctx->butx_result);
	m0_free(ctx->butx_data);
	m0_free(ctx);

	m0_be_tbq_fini(bbq);
	m0_free(bbq);
}

void m0_be_ut_tbq_1_1_1(void)      { be_ut_tbq(BE_UT_TBQ_1_1_1);      }
void m0_be_ut_tbq_100_1_1(void)    { be_ut_tbq(BE_UT_TBQ_100_1_1);    }
void m0_be_ut_tbq_100_1_10(void)   { be_ut_tbq(BE_UT_TBQ_100_1_10);   }
void m0_be_ut_tbq_100_10_1(void)   { be_ut_tbq(BE_UT_TBQ_100_10_1);   }
void m0_be_ut_tbq_100_10_10(void)  { be_ut_tbq(BE_UT_TBQ_100_10_10);  }
void m0_be_ut_tbq_10_100_1(void)   { be_ut_tbq(BE_UT_TBQ_10_100_1);   }
void m0_be_ut_tbq_10_100_5(void)   { be_ut_tbq(BE_UT_TBQ_10_100_5);   }
void m0_be_ut_tbq_10_1_100(void)   { be_ut_tbq(BE_UT_TBQ_10_1_100);   }
void m0_be_ut_tbq_10_5_100(void)   { be_ut_tbq(BE_UT_TBQ_10_5_100);   }
void m0_be_ut_tbq_10_100_100(void) { be_ut_tbq(BE_UT_TBQ_10_100_100); }


#undef M0_TRACE_SUBSYSTEM

/** @} end of be group */

/*
 *  Local variables:
 *  c-indentation-style: "K&R"
 *  c-basic-offset: 8
 *  tab-width: 8
 *  fill-column: 80
 *  scroll-step: 1
 *  End:
 */
/*
 * vim: tabstop=8 shiftwidth=8 noexpandtab textwidth=80 nowrap
 */
