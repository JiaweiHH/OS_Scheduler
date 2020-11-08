#include "sched.h"
#include <linux/sched/mm.h>
#include <linux/sched/topology.h>

#include <linux/latencytop.h>
#include <linux/cpumask.h>
#include <linux/cpuidle.h>
#include <linux/slab.h>
#include <linux/profile.h>
#include <linux/interrupt.h>
#include <linux/mempolicy.h>
#include <linux/migrate.h>
#include <linux/task_work.h>
#include <linux/sched/isolation.h>
#include <linux/sched.h>

#include <trace/events/sched.h>

#define NEW_TIMESLICE (HZ / 50)
#define INDEX_MAX 40
#define INDEX_MIN 0
#define INDEX_DEFAULT 20
#define MIN_DIFF 500
// #define MIN_UPDATE_INTERVAL 5

static int idle_balance(struct rq *this_rq, struct rq_flags *rf);

struct mlb_env {
	struct rq *src_rq;
	int src_cpu;

	struct rq *dst_rq;
	int dst_cpu;

	long imbalance;
	struct list_head tasks;//组织即将迁移的进程
};

static struct task_struct *
task_of(struct sched_new_entity *new_entity){
   return container_of(new_entity, struct task_struct, nt);
}

static int task_load(struct task_struct *p)
{
	return sched_prio_to_weight[p->nt.cur_weight_idx];
}

// static void 
// print_queue(struct new_rq *new_rq, int cpu){
   // struct rb_root *root = &new_rq->run_queue;
   // struct rb_node *node = rb_first(root);
   // struct sched_new_entity *nse;
   // struct task_struct *p;

   // if(new_rq->curr)
   //    printk("cpu: %d, tasks left->right. curr: %d, on_rq: %d,%d", cpu, new_rq->curr->pid, new_rq->curr->on_rq, new_rq->curr->nt.on_rq);
   // else
   //    printk("cpu: %d, tasks left->right!!!", cpu);
   

   // while(node){
   //    nse = rb_entry(node, struct sched_new_entity, run_node);
   //    p = task_of(nse);
   //    printk("cpu: %d, pid: %d, on_rq: %d, %d", cpu, p->pid, p->on_rq, nse->on_rq);
   //    node = rb_next(node);
   // }
   
   // printk("%d----------------------", cpu);
// }

/* todo Update task and its cfs_rq load average */
void update_load_avg(struct new_rq *new_rq, struct sched_new_entity *new_entity) {
}

static inline void
enqueue_runnable_load_sum(struct new_rq *new_rq, struct sched_new_entity *new_entity)
{
	new_rq->runnable_load_sum += sched_prio_to_weight[new_entity->cur_weight_idx];
}

static inline void
dequeue_runnable_load_sum(struct new_rq *new_rq, struct sched_new_entity *new_entity)
{
	new_rq->runnable_load_sum -= sched_prio_to_weight[new_entity->cur_weight_idx];
}

static void update_weight_index(struct task_struct *p) {
   struct sched_new_entity *nse = &p->nt;
   //内核线程mm为空
   if(!p->mm){
		nse->cur_weight_idx = INDEX_DEFAULT;
      return;
	}
   //第一次时间片用完 ，当前第一次统计RSS
	if(nse->lastRSS == 0){
		nse->lastRSS = get_mm_rss(p->mm);
      nse->cur_weight_idx = INDEX_DEFAULT;
      return;
	}
   //第n次时间片用完 ，当前第n次统计RSS(n >= 2)
	int index = nse->cur_weight_idx;
	int cur_rss = get_mm_rss(p->mm);
	
	if(cur_rss - nse->lastRSS > MIN_DIFF) {
		index--;
	} else if(nse->lastRSS - cur_rss > MIN_DIFF) {
		index++;
	}
	//调整至合法区间
   index = max(INDEX_MIN, index);
	index = min(INDEX_MAX - 1, index);
	nse->lastRSS = cur_rss;
   nse->cur_weight_idx = index;
}

//计算vruntime CFS为了避免浮点数运算造成的性能下降 加入位移运算 这里为了正确性 先用最原始的公式计算
static u64 update_vruntime(struct task_struct *p,struct rq *rq){
   struct sched_new_entity *nse = &p->nt;
   u64 delta = rq->clock - nse->exec_start;
   if(nse->cur_weight_idx == INDEX_DEFAULT)
      return delta;
   return delta * sched_prio_to_weight[INDEX_DEFAULT] / sched_prio_to_weight[nse->cur_weight_idx];
}

static u64 max_vruntime(u64 time1, u64 time2){
   s64 delta = (s64)(time2 - time1);
   if(delta < 0)
      return time1;
   return time2;
}

static u64 min_vruntime(u64 time1, u64 time2){
   s64 delta = (s64)(time2 - time1);
   if(delta < 0)
      return time2;
   return time1;
}

//每次update_curr的时候更新最小的vruntime. max{new_rq->vruntime, min{leftmost->vruntime, curr->vruntime}}
static void update_min_vruntime(struct new_rq *new_rq){
   struct sched_new_entity *nse;
   struct task_struct *p = new_rq->curr;
   struct rb_node *rb_left = rb_first(&new_rq->run_queue);

   u64 m_vruntime = p->nt.vruntime;

   if(rb_left){
      nse = rb_entry(rb_left, struct sched_new_entity, run_node);
      m_vruntime = min_vruntime(m_vruntime, nse->vruntime);
   }

   new_rq->min_vruntime = max_vruntime(m_vruntime, new_rq->min_vruntime);
   
}

void update_curr(struct rq *rq){
   struct new_rq *new_rq = &rq->nrq;
   struct task_struct *p = new_rq->curr;
   struct sched_new_entity *nse = &p->nt;
   nse->time_slice = NEW_TIMESLICE;

   // todo 更新权重时需要更新运行队列的负载值，后期改为pelt算法时需要删除
   dequeue_runnable_load_sum(new_rq, nse);
   update_weight_index(p);
   enqueue_runnable_load_sum(new_rq, nse);

   nse->vruntime += update_vruntime(p,rq);
   nse->exec_start = rq->clock;
   
   update_min_vruntime(new_rq);
}

void init_new_rq(struct new_rq *new_rq){
	new_rq->run_queue = RB_ROOT;
	new_rq->curr = NULL;
   new_rq->nr_running = 0;
   new_rq->min_vruntime = 0;
   new_rq->runnable_load_sum = 0;
}

int new_rq_empty(struct new_rq *nrq){
   return nrq->nr_running == 0;
}

bool compared_with_vruntime(struct sched_new_entity *new, struct sched_new_entity *temp){
   return (s64)(temp->vruntime - new->vruntime) > 0;
}

static void insert_rb_node(struct rb_root *root, struct sched_new_entity *se)
{
	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct sched_new_entity *entity;

	while (*link) {
		parent = *link;
		entity = rb_entry(parent, struct sched_new_entity, run_node);

		if (compared_with_vruntime(se, entity)) {
			link = &parent->rb_left;
		} else {
			link = &parent->rb_right;
		}
	}

	rb_link_node(&se->run_node, parent, link);
	rb_insert_color(&se->run_node, root);
}

static void 
enqueue_entity(struct new_rq *new_rq, struct sched_new_entity *new_entity){
   struct rb_node **link = &new_rq->run_queue.rb_node;
	struct rb_node *parent = NULL;
	struct sched_new_entity *entry;

   while (*link) {
		parent = *link;
		entry = rb_entry(parent, struct sched_new_entity, run_node);
		/*
		 * We dont care about collisions. Nodes with
		 * the same key stay together.
		 */
		if (compared_with_vruntime(new_entity, entry)) {
			link = &parent->rb_left;
		} else {
			link = &parent->rb_right;
			// leftmost = false;
		}
	}

   rb_link_node(&new_entity->run_node, parent, link);
	rb_insert_color(&new_entity->run_node,
			       &new_rq->run_queue);
}

static void
enqueue_task_new(struct rq *rq, struct task_struct *p, int flags){
   struct sched_new_entity *nse = &p->nt;
   struct new_rq *nrq = &rq->nrq;

   /* 
      放在enqueue更新vruntime这样可以保证两点  
      1.task_fork的时候不必考虑新的进程是不是在父进程的cpu上运行 
      2.dequeue的时候如果是迁移操作不必考虑两个cpu的vrumtime差距过大
   */
   nse->vruntime = nrq->min_vruntime;

   /*
	 * When enqueuing a sched_entity, we must:
	 *   - Update loads to have both entity and cfs_rq synced with now.
	 *   - Add its load to cfs_rq->runnable_avg
	 *   - For group_entity, update its weight to reflect the new share of
	 *     its group cfs_rq
	 *   - Add its new weight to cfs_rq->load.weight
	 */
	update_load_avg(nrq, nse);
	enqueue_runnable_load_sum(nrq, nse);

   if(nrq->curr != p)
      enqueue_entity(nrq, nse);

   if(nse->time_slice == 0 || nse->time_slice > NEW_TIMESLICE)
      nse->time_slice = NEW_TIMESLICE;
   
   nse->on_rq = 1;
   p->on_rq = 1;

   nrq->nr_running++;
   add_nr_running(rq, 1);
   // printk("cpu: %d, %d enqueue, new_rq have %d task\n", rq->cpu, p->pid, nrq->nr_running);
}

static void 
dequeue_entity(struct new_rq *new_rq, struct sched_new_entity *new_entity){
   rb_erase(&new_entity->run_node, &new_rq->run_queue);
}

static void 
dequeue_task_new(struct rq *rq, struct task_struct *p, int flags){
   struct sched_new_entity *nse = &p->nt;
   struct new_rq *nrq = &rq->nrq;

   // print_queue(nrq);
   if(nrq->curr != NULL)
      update_curr(rq); //看到网上说如果当前dequeue的进程就是正在运行的进程那么很有必要更新？？？

   /*
	 * When dequeuing a sched_entity, we must:
	 *   - Update loads to have both entity and cfs_rq synced with now.
	 *   - Substract its load from the cfs_rq->runnable_avg.
	 *   - Substract its previous weight from cfs_rq->load.weight.
	 *   - For group entity, update its weight to reflect the new share
	 *     of its group cfs_rq.
	 */
   update_load_avg(nrq, nse);
	dequeue_runnable_load_sum(nrq, nse);

   if(p != nrq->curr)
      dequeue_entity(nrq, nse);
   
   p->on_rq = 0;
   nse->on_rq = 0;
   nrq->nr_running--;
   sub_nr_running(rq, 1);
   // printk("cpu: %d, %d dequeue, new_rq have %d task\n", rq->cpu, p->pid, nrq->nr_running); 
}

static void yield_task_new(struct rq *rq){
   
}

static void check_preempt_curr_new(struct rq *rq, struct task_struct *p, int flags){
   return;
}

struct sched_new_entity *pick_next_entity(struct new_rq *new_rq){
   struct rb_node *left = rb_first(&new_rq->run_queue);
   if(left)
      return rb_entry(left, struct sched_new_entity, run_node);
   
   return NULL;
}

void set_next_entity(struct new_rq *new_rq, struct sched_new_entity *new_entity, int cpu){
   if(new_entity->on_rq){
      dequeue_entity(new_rq, new_entity);
   }
   new_rq->curr = task_of(new_entity);
}

static struct task_struct *
pick_next_task_new(struct rq *rq, struct task_struct *prev, struct rq_flags *rf){
   struct new_rq *new_rq = &rq->nrq;
	struct sched_new_entity *nse;
	struct task_struct *p;
	int new_tasks;

again:
   if(new_rq->nr_running != 0){
      put_prev_task(rq, prev);
      nse = pick_next_entity(new_rq);
      set_next_entity(new_rq, nse, rq->cpu);
      p = task_of(nse);
      p->nt.exec_start = rq->clock;
      return p;
   }

   new_tasks = idle_balance(rq, rf);

   if(new_tasks < 0)
      return RETRY_TASK;
   if(new_tasks > 0)
      goto again;
   
   return NULL;
}

static void task_tick_new(struct rq *rq, struct task_struct *p, int queued){
   struct sched_new_entity *nse;
   nse = &p->nt;
   if(--nse->time_slice){
      return;
   }
   //time_slice == 0
   update_curr(rq);
   

   struct new_rq *nrq = &rq->nrq;

   if(nrq->nr_running > 1){
      // printk("cpu: %d, %d need sched", rq->cpu, p->pid);
      resched_curr(rq);
   }
} 

static void put_prev_task_new(struct rq *rq, struct task_struct *prev){
   struct sched_new_entity *nse = &prev->nt;

   struct new_rq *new_rq= &rq->nrq;
   if(nse->on_rq){
      update_curr(rq);
      enqueue_entity(new_rq, &prev->nt);
   }
   
   new_rq->curr = NULL;
}

static void set_curr_task_new(struct rq *rq){
   struct new_rq *new_rq = &rq->nrq;
   struct sched_new_entity *new_entity = &rq->curr->nt;
   set_next_entity(new_rq, new_entity, rq->cpu);
}

static void task_fork_new(struct task_struct *p){
   p->nt.time_slice = NEW_TIMESLICE;
}
static void switched_from_new(struct rq *this_rq, struct task_struct *task){
   
}
static void switched_to_new(struct rq *this_rq, struct task_struct *task){
   task->nt.time_slice = NEW_TIMESLICE;
}
static void prio_changed_new(struct rq *this_rq, struct task_struct *task,
			     int oldprio){

}
static unsigned int get_rr_interval_new(struct rq *rq, struct task_struct *task){
   return NEW_TIMESLICE;
}



#ifdef CONFIG_SMP

bool is_migrate_task(struct task_struct *task, struct rq *this_rq, struct rq *target_rq){
   if(!cpumask_test_cpu(this_rq->cpu, &task->cpus_allowed))
      return false;
   if(task_running(target_rq, task)) //task_running开启了SMP时判断on_cpu字段
      return false;
   return true;
}

void trigger_my_load_balance(struct rq *rq){
   if(time_after_eq(jiffies, rq->next_balance))
      raise_softirq(SCHED_SOFTIRQ);
}

static int select_task_rq_new(struct task_struct *p, int prev_cpu, int sd_flag, int flags) {
   struct rq *rq;
   unsigned int cpu;
   unsigned int target_cpu = prev_cpu;
   u64 target_rq_load = -1;

   for_each_cpu_and(cpu, cpu_online_mask, &p->cpus_allowed) {
      rq = cpu_rq(cpu);
      if (target_rq_load < 0 || rq->nrq.runnable_load_sum < target_rq_load) { // 选择负载最低的CPU
         target_cpu = cpu;
         target_rq_load = rq->nrq.runnable_load_sum;
      }
      if (rq->nr_running == 0) break; // 有空闲的CPU直接选择
   }
   
   return target_cpu;
}

/* Assumes rq->lock is held */
static void rq_online_new(struct rq *rq){
}

/* Assumes rq->lock is held */
static void rq_offline_new(struct rq *rq){
}

struct rq *find_busiest_rq(int this_cpu){
   int cpu;
   u64 target_rq_load = 0;
   struct rq *target_rq = NULL;

   for_each_online_cpu(cpu){
      if(cpu == this_cpu)
         continue;
      struct rq *rq = cpu_rq(cpu);
      if(rq->nrq.nr_running > 1 && rq->nrq.runnable_load_sum > target_rq_load) {
         target_rq_load = rq->nrq.runnable_load_sum;
         target_rq = rq;
      }
   }


   return target_rq;
}

static void calculate_imbalance(struct mlb_env *env)
{
	env->imbalance = (env->src_rq->nrq.runnable_load_sum -
			  env->dst_rq->nrq.runnable_load_sum) /
			 2;
}

static void attach_tasks(struct mlb_env *env)
{
	// struct rb_root *root = &env->tasks;
	// struct rb_node *node = root->rb_node;
	struct task_struct *p;
	struct sched_new_entity *se;

	while (!list_empty(&env->tasks)) {
      se = list_entry(env->tasks.next,struct sched_new_entity,list_node);
		p = task_of(se);
      list_del(&se->list_node);

		// p->on_rq = TASK_ON_RQ_QUEUED;
		activate_task(env->dst_rq, p, 0);
	}
}

static int detach_tasks(struct mlb_env *env)
{
	struct rb_root *root = &env->src_rq->nrq.run_queue;
	struct rb_node *node = root->rb_node;
	struct sched_new_entity *se;
	struct task_struct *p;
	unsigned long load;
	int detached = 0;

	if (env->imbalance <= 0) {
		return 0;
	}

	while (node) {
		se = rb_entry(node, struct sched_new_entity, run_node);
		p = task_of(se);
		if (!is_migrate_task(p, env->dst_rq, env->src_rq)) {
			node = rb_next(node);
			continue;
		}

		load = task_load(p);
		deactivate_task(env->src_rq, p, 0);
		// p->on_rq = TASK_ON_RQ_MIGRATING;
		set_task_cpu(p, env->dst_cpu);

      list_add_tail(&se->list_node,&env->tasks);
		detached++;

		env->imbalance -= load;
		if (env->imbalance <= 0)
			break;

		node = rb_next(node);
	}

	return detached;
}

static __latent_entropy void run_my_load_balance(struct softirq_action *h)
{
	struct rq *this_rq = this_rq();
   unsigned long next_balance = jiffies + 60*HZ;  //触发周期
   int ld_num = 0;

	struct mlb_env env = {
		.dst_cpu = smp_processor_id(),
		.dst_rq = this_rq,
	};

   INIT_LIST_HEAD(&env.tasks);

   struct rq *busiest_rq = find_busiest_rq(this_rq->cpu);

   if(busiest_rq == NULL){
      return;
   }

   env.src_cpu = busiest_rq->cpu;
	env.src_rq = busiest_rq;
	calculate_imbalance(&env);

   // //需要在遍历链表获取migrate_task之前加锁，不然的话会导致当运行到删除进程的时候另一个CPU将migrate_task设置为正在运行的进程了
   raw_spin_lock_irq(&busiest_rq->lock);   
   ld_num = detach_tasks(&env);
   raw_spin_unlock(&busiest_rq->lock);


   if(ld_num > 0){
      // printk("load_balance:ld_num=%d\n",ld_num);
	   raw_spin_lock(&env.dst_rq->lock);
      attach_tasks(&env);
	   raw_spin_unlock_irq(&env.dst_rq->lock);
   }
   this_rq->next_balance = next_balance;
   
}

static void migrate_task_rq_new(struct task_struct *p){
   // printk("%d. next step set cpu\n");
}

static void task_dead_new(struct task_struct *p){}

/*
   到一个就绪队列最长的cpu，从中挑选一个进程迁移到this_rq.
   不会出现说cpu0想在cpu1获取进程，cpu1想在cpu0获取进程的情况从而不会引发死锁. 
   因为目标是找到最长的rq，而要出现上述情况两个rq必须一样长，但是其中一个为0，因此不可能出现
*/
static int idle_balance(struct rq *this_rq, struct rq_flags *rf){
   int this_cpu = this_rq->cpu;
   int pulled_task = 0;

   if (!cpu_active(this_cpu))
		return 0;

   struct mlb_env env = {
		.dst_cpu = smp_processor_id(),
		.dst_rq = this_rq,
	};

   INIT_LIST_HEAD(&env.tasks);

   struct rq *target_rq = find_busiest_rq(this_cpu);
   if(target_rq != NULL)
      raw_spin_lock(&target_rq->lock); //不需要对this_rq加锁，因为在pick_next_task被调用之前就已经获取了this_rq的锁
   else
      return 0;

   env.src_cpu = target_rq->cpu;
	env.src_rq = target_rq;
	calculate_imbalance(&env);

   pulled_task = detach_tasks(&env);

   if(pulled_task > 0){
      // printk("idle balance:pull_task=%d\n",pulled_task);
      attach_tasks(&env);
   }

   raw_spin_unlock(&target_rq->lock);

   if (this_rq->nr_running != this_rq->nrq.nr_running) //这个判断可能是无效的，因为this_rq的锁一直被持有
		pulled_task = -1;

   return pulled_task;
}

#endif /* COMFIG_SMP */

#ifdef CONFIG_FAIR_GROUP_SCHED
void task_change_group_new(struct task_struct *p, int type){
}
#endif



const struct sched_class new_sched_class = {
   .next           = &fair_sched_class,
   .enqueue_task       = enqueue_task_new,
   .dequeue_task       = dequeue_task_new,
   .yield_task     = yield_task_new,
   // .yield_to_task		= yield_to_task_new,

   .check_preempt_curr = check_preempt_curr_new,

   .pick_next_task     = pick_next_task_new,
   .put_prev_task      = put_prev_task_new,

#ifdef CONFIG_SMP
   .select_task_rq     = select_task_rq_new,
   .migrate_task_rq	= migrate_task_rq_new,

   .task_dead		= task_dead_new,
   .set_cpus_allowed       = set_cpus_allowed_common,

   .rq_online              = rq_online_new,
   .rq_offline             = rq_offline_new,
#endif

   .set_curr_task          = set_curr_task_new,
   .task_tick      = task_tick_new,
   .task_fork              = task_fork_new,

   .switched_from          = switched_from_new,
   .switched_to        = switched_to_new,

   .prio_changed       = prio_changed_new,

   .get_rr_interval    = get_rr_interval_new,
   .update_curr        = update_curr, 
// #ifdef CONFIG_FAIR_GROUP_SCHED
//    .task_change_group            = task_change_group_new
// #endif
};

// #endif /* CONFIG_SCHED_NEW */

__init void init_sched_new_class(void)
{
#ifdef CONFIG_SMP
	open_softirq(SCHED_SOFTIRQ, run_my_load_balance);
#endif /* SMP */

}