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


void m0_be_ut_tbq(void)
{
	struct m0_be_tbq_cfg  bbq_cfg = {
		.bqc_q_size_max       = 0x1000,
		.bqc_producers_nr_max = 0x100,
		.bqc_consumers_nr_max = 0x200,
	};
	struct m0_be_tbq     *bbq;
	int                   rc;

	M0_ALLOC_PTR(bbq);
	M0_ASSERT(bbq != NULL);
	rc = m0_be_tbq_init(bbq, &bbq_cfg);
	M0_ASSERT_INFO(rc == 0, "rc=%d", rc);
	m0_be_tbq_fini(bbq);
	m0_free(bbq);
}

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
