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

#include "ut/threads.h"         /* m0_be_theads_start */
#include "ut/ut.h"              /* M0_UT_ASSERT */


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
	uint32_t butc_items_nr;
};

struct be_ut_tbq_thread_param {
	bool                butqp_producer;
	struct m0_semaphore butqp_sem_start;
};

#define BE_UT_TBQ_TEST(q_size_max, producers, consumers, items_nr) \
{                                               \
	.butc_q_size_max = (q_size_max),        \
	.butc_producers  = (producers),         \
	.butc_consumers  = (consumers),         \
	.butc_items_nr   = (items_nr)           \
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

static void be_ut_tbq_thread(void *_param)
{
	struct be_ut_tbq_thread_param *param = _param;

	m0_semaphore_down(&param->butqp_sem_start);
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
	struct m0_be_tbq              *bbq;
	int                            rc;
	uint32_t                       threads_nr;
	uint32_t                       i;

	M0_ALLOC_PTR(bbq);
	M0_ASSERT(bbq != NULL);
	rc = m0_be_tbq_init(bbq, &bbq_cfg);
	M0_ASSERT_INFO(rc == 0, "rc=%d", rc);

	threads_nr = test_cfg->butc_producers + test_cfg->butc_consumers;
	M0_ALLOC_ARR(params, threads_nr);
	for (i = 0; i < threads_nr; ++i) {
		params[i].butqp_producer = i < test_cfg->butc_producers;
		m0_semaphore_init(&params[i].butqp_sem_start, 0);
	}
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
