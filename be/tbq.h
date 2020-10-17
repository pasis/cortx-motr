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


#pragma once

#ifndef __MOTR_BE_TX_BULK_QUEUE_H__
#define __MOTR_BE_TX_BULK_QUEUE_H__

/**
 * @defgroup be
 *
 * Queues use lists in this way: items are added at tail and are removed from
 * the head.
 *
 * @{
 */

#include "lib/types.h"          /* m0_bcount_t */
#include "lib/tlist.h"          /* m0_tl */
#include "lib/mutex.h"          /* m0_mutex */

#include "be/tx_credit.h"       /* m0_be_tx_credit */


struct m0_be_op;
struct be_tx_bulk_wait_op;

struct m0_be_tbq_data {
	void                   *bbd_user;
	struct m0_be_tx_credit  bbd_credit;
	m0_bcount_t             bbd_payload_size;
};

#define BETBQD_F "(qdata=%p bbd_user=%p bbd_credit="BETXCR_F" " \
	"bbd_payload_size=%"PRIu64")"

#define BETBQD_P(bqd) (bqd), (bqd)->bbd_user, BETXCR_P(&(bqd)->bbd_credit), \
	(bqd)->bbd_payload_size

struct m0_be_tbq_item {
	struct m0_be_tbq_data bbi_data;
	uint64_t                   bbi_magic;
	/** tbq_tl, m0_be_tbq::bbq_q */
	struct m0_tlink            bbi_link;
};

struct m0_be_tbq_cfg {
	uint32_t bqc_q_size_max;
	uint32_t bqc_producers_nr_max;
	uint32_t bqc_consumers_nr_max;
};

/**
 */
struct m0_be_tbq {
	struct m0_be_tbq_cfg  bbq_cfg;
	struct m0_mutex                 bbq_lock;

	/** tbq_tl, m0_be_tbq_item::bbi_link */
	struct m0_tl                    bbq_q;
	struct m0_tl                    bbq_q_unused;

	/** tbqop_tl, m0_be_tbq_item::bbi_link */
	struct m0_tl                    bbq_op_put;
	struct m0_tl                    bbq_op_put_unused;
	struct m0_tl                    bbq_op_get;
	struct m0_tl                    bbq_op_get_unused;

	/** Pre-allocated array of qitems */
	struct m0_be_tbq_item     *bbq_qitems;
	/** Is used to wait in m0_be_tbq_get() */
	struct be_tx_bulk_wait_op      *bbq_ops_get;
	/** Is used to wait in m0_be_tbq_put() */
	struct be_tx_bulk_wait_op      *bbq_ops_put;

	uint32_t                        bbq_enqueued;
	uint32_t                        bbq_dequeued;
};

#define BETBQ_F "(queue=%p bbq_enqueued=%"PRIu32" bbq_dequeued=%"PRIu32")"
#define BETBQ_P(bbq) (bbq), (bbq)->bbq_enqueued, (bbq)->bbq_dequeued

M0_INTERNAL int m0_be_tbq_init(struct m0_be_tbq     *bbq,
                                         struct m0_be_tbq_cfg *cfg);
M0_INTERNAL void m0_be_tbq_fini(struct m0_be_tbq *bbq);

M0_INTERNAL void m0_be_tbq_lock(struct m0_be_tbq *bbq);
M0_INTERNAL void m0_be_tbq_unlock(struct m0_be_tbq *bbq);

M0_INTERNAL void
m0_be_tbq_put(struct m0_be_tbq   *bbq,
                        struct m0_be_op              *op,
                        void                         *user,
                        const struct m0_be_tx_credit *credit,
                        m0_bcount_t                   payload_size);
M0_INTERNAL void
m0_be_tbq_get(struct m0_be_tbq  *bbq,
                        struct m0_be_op             *op,
                        void                       **user,
                        struct m0_be_tx_credit      *credit,
                        m0_bcount_t                 *payload_size);
M0_INTERNAL bool
m0_be_tbq_peek(struct m0_be_tbq  *bbq,
                        void                        **user,
                        struct m0_be_tx_credit       *credit,
                        m0_bcount_t                  *payload_size);

/** @} end of be group */
#endif /* __MOTR_BE_TX_BULK_QUEUE_H__ */

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
