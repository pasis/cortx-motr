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

#include "lib/errno.h"          /* ENOMEM */
#include "lib/memory.h"         /* M0_ALLOC_ARR */

#include "be/op.h"              /* m0_be_op_active */


struct be_tbq_item {
	struct m0_be_tbq_data bbi_data;
	uint64_t              bbi_magic;
	/** tbq_tl, m0_be_tbq::bbq_q */
	struct m0_tlink       bbi_link;
};

struct be_tbq_wait_op {
	struct m0_be_op       *bwo_op;
	struct be_tbq_item    *bwo_bqi;
	struct m0_be_tbq_data *bwo_data;
	uint64_t               bwo_magic;
	struct m0_tlink        bwo_link;
};

M0_TL_DESCR_DEFINE(tbq, "m0_be_tbq::bbq_q*[]", static,
		   struct be_tbq_item, bbi_link, bbi_magic,
		   /* XXX */ 1, /* XXX */ 2);
M0_TL_DEFINE(tbq, static, struct be_tbq_item);

M0_TL_DESCR_DEFINE(tbqop, "m0_be_tbq::bbq_op_*[]", static,
		   struct be_tbq_wait_op, bwo_link, bwo_magic,
		   /* XXX */ 3, /* XXX */ 4);
M0_TL_DEFINE(tbqop, static, struct be_tbq_wait_op);


M0_INTERNAL int m0_be_tbq_init(struct m0_be_tbq     *bbq,
                               struct m0_be_tbq_cfg *cfg)
{
	uint32_t qitems_nr;
	uint32_t i;

	M0_ENTRY("bbq=%p bqc_q_size_max=%"PRIu32" "
		 "bqc_producers_nr_max=%"PRIu32" bqc_consumers_nr_max=%"PRIu32,
		 bbq, cfg->bqc_q_size_max,
		 cfg->bqc_producers_nr_max, cfg->bqc_consumers_nr_max);

	bbq->bbq_cfg = *cfg;
	bbq->bbq_enqueued = 0;
	bbq->bbq_dequeued = 0;
	qitems_nr = bbq->bbq_cfg.bqc_q_size_max +
		    bbq->bbq_cfg.bqc_producers_nr_max;
	M0_ALLOC_ARR(bbq->bbq_qitems, qitems_nr);
	M0_ALLOC_ARR(bbq->bbq_ops_put, bbq->bbq_cfg.bqc_producers_nr_max);
	M0_ALLOC_ARR(bbq->bbq_ops_get, bbq->bbq_cfg.bqc_consumers_nr_max);
	if (bbq->bbq_qitems == NULL ||
	    bbq->bbq_ops_put == NULL ||
	    bbq->bbq_ops_get == NULL) {
		m0_free(bbq->bbq_ops_get);
		m0_free(bbq->bbq_ops_put);
		m0_free(bbq->bbq_qitems);
		return M0_ERR(-ENOMEM);
	}
	m0_mutex_init(&bbq->bbq_lock);
	tbqop_tlist_init(&bbq->bbq_op_put_unused);
	for (i = 0; i < bbq->bbq_cfg.bqc_producers_nr_max; ++i) {
		tbqop_tlink_init_at_tail(&bbq->bbq_ops_put[i],
					 &bbq->bbq_op_put_unused);
	}
	tbqop_tlist_init(&bbq->bbq_op_put);
	tbqop_tlist_init(&bbq->bbq_op_get_unused);
	for (i = 0; i < bbq->bbq_cfg.bqc_consumers_nr_max; ++i) {
		tbqop_tlink_init_at_tail(&bbq->bbq_ops_get[i],
					 &bbq->bbq_op_get_unused);
	}
	tbqop_tlist_init(&bbq->bbq_op_get);
	tbq_tlist_init(&bbq->bbq_q_unused);
	for (i = 0; i < qitems_nr; ++i)
		tbq_tlink_init_at_tail(&bbq->bbq_qitems[i], &bbq->bbq_q_unused);
	tbq_tlist_init(&bbq->bbq_q);
	return M0_RC(0);
}

M0_INTERNAL void m0_be_tbq_fini(struct m0_be_tbq *bbq)
{
	struct be_tbq_wait_op  *bwo;
	struct be_tbq_item     *bqi;
	uint32_t                qitems_nr;
	uint32_t                i;

	M0_ENTRY("bbq="BETBQ_F, BETBQ_P(bbq));
	M0_ASSERT_INFO(bbq->bbq_enqueued == bbq->bbq_dequeued,
	               "bbq="BETBQ_F, BETBQ_P(bbq));

	m0_tl_for(tbq, &bbq->bbq_q, bqi) {
		M0_LOG(M0_ERROR, "bqd="BETBQD_F, BETBQD_P(&bqi->bbi_data));
	} m0_tl_endfor;
	tbq_tlist_fini(&bbq->bbq_q);
	m0_tl_for(tbqop, &bbq->bbq_op_get, bwo) {
		M0_LOG(M0_ERROR, "bbq=%p bwo_data=%p", bbq, bwo->bwo_data);
	} m0_tl_endfor;
	tbqop_tlist_fini(&bbq->bbq_op_get);
	for (i = 0; i < bbq->bbq_cfg.bqc_consumers_nr_max; ++i)
		tbqop_tlink_del_fini(&bbq->bbq_ops_get[i]);
	tbqop_tlist_fini(&bbq->bbq_op_get_unused);
	/* if there was nothing in bbq_q then the following list is empty */
	tbqop_tlist_fini(&bbq->bbq_op_put);
	for (i = 0; i < bbq->bbq_cfg.bqc_producers_nr_max; ++i)
		tbqop_tlink_del_fini(&bbq->bbq_ops_put[i]);
	tbqop_tlist_fini(&bbq->bbq_op_put_unused);
	qitems_nr = bbq->bbq_cfg.bqc_q_size_max +
		    bbq->bbq_cfg.bqc_producers_nr_max;
	for (i = 0; i < qitems_nr; ++i)
		tbq_tlink_del_fini(&bbq->bbq_qitems[i]);
	tbq_tlist_fini(&bbq->bbq_q_unused);
	m0_mutex_fini(&bbq->bbq_lock);
	m0_free(bbq->bbq_ops_put);
	m0_free(bbq->bbq_ops_get);
	m0_free(bbq->bbq_qitems);
	M0_LEAVE();
}

M0_INTERNAL void m0_be_tbq_lock(struct m0_be_tbq *bbq)
{
	m0_mutex_lock(&bbq->bbq_lock);
}

M0_INTERNAL void m0_be_tbq_unlock(struct m0_be_tbq *bbq)
{
	m0_mutex_unlock(&bbq->bbq_lock);
}

static uint32_t be_tbq_q_size(struct m0_be_tbq *bbq)
{
	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_ASSERT_INFO(bbq->bbq_enqueued >= bbq->bbq_dequeued,
	               "bbq="BETBQ_F, BETBQ_P(bbq));
	return bbq->bbq_enqueued - bbq->bbq_dequeued;
}

static bool be_tbq_is_empty(struct m0_be_tbq *bbq)
{
	return be_tbq_q_size(bbq) == 0;
}

static bool be_tbq_is_full(struct m0_be_tbq *bbq)
{
	return be_tbq_q_size(bbq) >= bbq->bbq_cfg.bqc_q_size_max;
}

static struct be_tbq_item *be_tbq_q_put(struct m0_be_tbq            *bbq,
                                        const struct m0_be_tbq_data *bqd)
{
	struct be_tbq_item *bqi;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));

	bqi = tbq_tlist_head(&bbq->bbq_q_unused);
	bqi->bbi_data = *bqd;
	tbq_tlist_move_tail(&bbq->bbq_q, bqi);
	++bbq->bbq_enqueued;
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F,
		 BETBQ_P(bbq), BETBQD_P(&bqi->bbi_data));
	return bqi;
}

static void be_tbq_q_peek(struct m0_be_tbq      *bbq,
			  struct m0_be_tbq_data *bqd)
{
	struct be_tbq_item *bqi;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!be_tbq_is_empty(bbq));

	bqi = tbq_tlist_head(&bbq->bbq_q);
	*bqd = bqi->bbi_data;
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F, BETBQ_P(bbq), BETBQD_P(bqd));
}

static void be_tbq_q_get(struct m0_be_tbq      *bbq,
                         struct m0_be_tbq_data *bqd)
{
	struct be_tbq_item *bqi;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!be_tbq_is_empty(bbq));

	bqi = tbq_tlist_head(&bbq->bbq_q);
	*bqd = bqi->bbi_data;
	tbq_tlist_move(&bbq->bbq_q_unused, bqi);
	++bbq->bbq_dequeued;
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F, BETBQ_P(bbq), BETBQD_P(bqd));
}

static void be_tbq_op_put(struct m0_be_tbq   *bbq,
                              struct m0_be_op    *op,
                              struct be_tbq_item *bqi)
{
	struct be_tbq_wait_op *bwo;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!tbqop_tlist_is_empty(&bbq->bbq_op_put_unused));

	bwo = tbqop_tlist_head(&bbq->bbq_op_put_unused);
	bwo->bwo_bqi = bqi;
	bwo->bwo_op  = op;
	tbqop_tlist_move_tail(&bbq->bbq_op_put, bwo);
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F,
		 BETBQ_P(bbq), BETBQD_P(&bqi->bbi_data));
}

static void be_tbq_op_put_done(struct m0_be_tbq *bbq)
{
	struct be_tbq_wait_op *bwo;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!tbqop_tlist_is_empty(&bbq->bbq_op_put));

	bwo = tbqop_tlist_head(&bbq->bbq_op_put);
	m0_be_op_done(bwo->bwo_op);
	tbqop_tlist_move(&bbq->bbq_op_put_unused, bwo);
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F,
		 BETBQ_P(bbq), BETBQD_P(&bwo->bwo_bqi->bbi_data));
}

static bool be_tbq_op_put_is_waiting(struct m0_be_tbq *bbq)
{
	return !tbqop_tlist_is_empty(&bbq->bbq_op_put);
}

static void be_tbq_op_get(struct m0_be_tbq      *bbq,
                              struct m0_be_op       *op,
                              struct m0_be_tbq_data *bqd)
{
	struct be_tbq_wait_op *bwo;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!tbqop_tlist_is_empty(&bbq->bbq_op_get_unused));

	bwo = tbqop_tlist_head(&bbq->bbq_op_get_unused);
	bwo->bwo_data = bqd;
	bwo->bwo_op   = op;
	tbqop_tlist_move_tail(&bbq->bbq_op_get, bwo);
	M0_LEAVE("bbq=%p bwo_data=%p", bbq, bwo->bwo_data);
}

static void be_tbq_op_get_done(struct m0_be_tbq      *bbq,
                                   struct m0_be_tbq_data *bqd)
{
	struct be_tbq_wait_op *bwo;

	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));
	M0_PRE(!tbqop_tlist_is_empty(&bbq->bbq_op_get));

	bwo = tbqop_tlist_head(&bbq->bbq_op_get);
	*bwo->bwo_data = *bqd;
	m0_be_op_done(bwo->bwo_op);
	tbqop_tlist_move(&bbq->bbq_op_get_unused, bwo);
	M0_LEAVE("bbq="BETBQ_F"bwo_data=%p bqd="BETBQD_F,
		 BETBQ_P(bbq), bwo->bwo_data, BETBQD_P(bqd));
}

static bool be_tbq_op_get_is_waiting(struct m0_be_tbq *bbq)
{
	return !tbqop_tlist_is_empty(&bbq->bbq_op_get);
}

M0_INTERNAL void m0_be_tbq_put(struct m0_be_tbq      *bbq,
                               struct m0_be_op       *op,
                               struct m0_be_tbq_data *bqd)
{
	struct m0_be_tbq_data  bqd_on_stack;
	struct be_tbq_item    *bqi;
	bool                   was_full;

	M0_ENTRY("bbq="BETBQ_F" bqd="BETBQD_F, BETBQ_P(bbq), BETBQD_P(bqd));
	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));

	m0_be_op_active(op);
	was_full = be_tbq_is_full(bbq);
	bqi = be_tbq_q_put(bbq, bqd);
	if (was_full) {
		be_tbq_op_put(bbq, op, bqi);
	} else {
		m0_be_op_done(op);
	}
	if (be_tbq_op_get_is_waiting(bbq)) {
		/*
		 * Shortcut for this case hasn't been not done intentionally.
		 * It's much easier to look at the logs when all items are
		 * always added to the queue.
		 */
		be_tbq_q_get(bbq, &bqd_on_stack);
		be_tbq_op_get_done(bbq, &bqd_on_stack);
	}
}

M0_INTERNAL void m0_be_tbq_get(struct m0_be_tbq      *bbq,
                               struct m0_be_op       *op,
                               struct m0_be_tbq_data *bqd)
{
	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));

	m0_be_op_active(op);
	if (be_tbq_is_empty(bbq) || be_tbq_op_get_is_waiting(bbq)) {
		be_tbq_op_get(bbq, op, bqd);
		return;
	}
	be_tbq_q_get(bbq, bqd);
	m0_be_op_done(op);
	if (be_tbq_op_put_is_waiting(bbq))
		be_tbq_op_put_done(bbq);
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F, BETBQ_P(bbq), BETBQD_P(bqd));
}

M0_INTERNAL bool m0_be_tbq_peek(struct m0_be_tbq      *bbq,
                                struct m0_be_tbq_data *bqd)
{
	M0_PRE(m0_mutex_is_locked(&bbq->bbq_lock));

	if (be_tbq_is_empty(bbq) ||
	    be_tbq_op_get_is_waiting(bbq)) {
		M0_LOG(M0_DEBUG, "bbq=%p the queue is empty", bbq);
		return false;
	}
	be_tbq_q_peek(bbq, bqd);
	M0_LEAVE("bbq="BETBQ_F" bqd="BETBQD_F, BETBQ_P(bbq), BETBQD_P(bqd));
	return true;
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
