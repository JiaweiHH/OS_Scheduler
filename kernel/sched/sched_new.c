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

#define NEW_TIMESLICE 25
#define INDEX_MAX 40
#define INDEX_MIN 0
#define INDEX_DEFAULT 20
#define MIN_DIFF 500
// #define MIN_UPDATE_INTERVAL 5

static int idle_balance(struct rq *this_rq, struct rq_flags *rf);

static struct task_struct *
task_of(struct sched_new_entity *new_entity){
   return container_of(new_entity, struct task_struct, nt);
}

static void update_weight_index(struct task_struct *p){
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

//计算vruntime 直接参考奔跑吧Linux内核 p350
static u64 update_vruntime(struct task_struct *p,struct rq *rq){
   struct sched_new_entity *nse = &p->nt;
   u64 delta = rq->clock - nse->exec_start;
   if(nse->cur_weight_idx == INDEX_DEFAULT)
      return delta;
   
   unsigned long weight_ = scale_load(sched_prio_to_weight[nse->cur_weight_idx]);
   u32 in_weight_ = sched_prio_to_wmult[nse->cur_weight_idx];
   int shift = 32;
   u64 fact = weight_;
   fact = (u64)(u32)fact * in_weight_;
   while(fact >> 32) {
      fact >>= 1;
      shift--;
   }

   return (u64)((delta * fact) >> shift);
}

void update_curr(struct rq *rq){
   struct new_rq *new_rq = &rq->nrq;
   struct task_struct *p = new_rq->curr;
   struct sched_new_entity *nse = &p->nt;
   nse->time_slice = NEW_TIMESLICE;

   update_weight_index(p);
   nse->vruntime += update_vruntime(p,rq);
   nse->exec_start = rq->clock;
   
}

void init_new_rq(struct new_rq *new_rq){
	new_rq->run_queue = RB_ROOT;
	new_rq->curr = NULL;
   new_rq->nr_running = 0;
   new_rq->min_vruntime = 0;
}

int new_rq_empty(struct new_rq *nrq){
   return nrq->nr_running == 0;
}

bool compared_with_vruntime(struct sched_new_entity *new, struct sched_new_entity *temp){
   return (s64)(temp->vruntime - new->vruntime) > 0;
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
   p->nt.vruntime = current->nt.vruntime;
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

static int select_task_rq_new(struct task_struct *p, int prev_cpu, int sd_flag, int flags){
   unsigned int cpu;
   unsigned int temp_cpu = prev_cpu, amount_tasks = 65536;

   for_each_online_cpu(cpu){
      struct rq *rq = cpu_rq(cpu);
      if(rq->nr_running < amount_tasks && cpumask_test_cpu(cpu, &p->cpus_allowed)){
         temp_cpu = cpu;
         amount_tasks = rq->nrq.nr_running;
      }
   }
   return temp_cpu;
}

/* Assumes rq->lock is held */
static void rq_online_new(struct rq *rq){
}

/* Assumes rq->lock is held */
static void rq_offline_new(struct rq *rq){
}

struct rq *find_busiest_rq(int this_cpu){
   int cpu, nr_task = 0;
   struct rq *target_rq = NULL;

   for_each_online_cpu(cpu){
      if(cpu == this_cpu)
         continue;
      struct rq *rq = cpu_rq(cpu);
      if(rq->nrq.nr_running > 1 && rq->nrq.nr_running > nr_task){
         nr_task = rq->nrq.nr_running;
         target_rq = rq;
      }
   }


   return target_rq;
}

static __latent_entropy void run_my_load_balance(struct softirq_action *h)
{
	struct rq *this_rq = this_rq();
   unsigned long next_balance = jiffies + 60*HZ;  //触发周期
   struct rq_flags rf;
   struct rq *busiest_rq = find_busiest_rq(this_rq->cpu);

   if(busiest_rq == NULL){
      return;
   }

   // //需要在遍历链表获取migrate_task之前加锁，不然的话会导致当运行到删除进程的时候另一个CPU将migrate_task设置为正在运行的进程了
   raw_spin_lock_irq(&busiest_rq->lock);
   struct rb_root *root = &busiest_rq->nrq.run_queue;
   struct rb_node *node = root->rb_node;
   struct sched_new_entity *se;
   struct task_struct *migrate_task = NULL;

   while(node){
      se = rb_entry(node, struct sched_new_entity, run_node);
      struct task_struct *p = task_of(se);
      if(is_migrate_task(p, this_rq, busiest_rq)){
         migrate_task = p;
         break;
      }
      node = rb_next(node);
   }
   if(migrate_task == NULL){
      raw_spin_unlock_irq(&busiest_rq->lock);
      return;
   }
      
   // printk("%d softirq load_balance触发\n", this_rq->cpu);
   
   deactivate_task(busiest_rq, migrate_task, 0);
   set_task_cpu(migrate_task, this_rq->cpu);
   raw_spin_unlock(&busiest_rq->lock);

   raw_spin_lock(&this_rq->lock);
   activate_task(this_rq, migrate_task, 0);
   this_rq->next_balance = next_balance;
   
   raw_spin_unlock_irq(&this_rq->lock);
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

   struct rq *target_rq = find_busiest_rq(this_cpu);
   if(target_rq != NULL)
      raw_spin_lock(&target_rq->lock); //不需要对this_rq加锁，因为在pick_next_task被调用之前就已经获取了this_rq的锁
   else
      return 0;
      
   // struct list_head *queue = &target_nrq->run_queue;
   struct rb_root *root = &target_rq->nrq.run_queue;
   struct rb_node *node = root->rb_node;
   int target_cpu = target_rq->cpu;

   struct sched_new_entity *temp_se;
   struct task_struct *migrate_task = NULL;

   while(node){
      temp_se = rb_entry(node, struct sched_new_entity, run_node);
      struct task_struct *p = container_of(temp_se, struct task_struct, nt);
      
      if(is_migrate_task(p, this_rq, target_rq)){
         migrate_task = p;
         break;
      }

      node = rb_next(node);
   }
   if(migrate_task){
         deactivate_task(target_rq, migrate_task, 0);
         // del_task->on_rq = TASK_ON_RQ_MIGRATING; //CFS中迁移进程的时候设置了这个状态位, 测试发现可以不加
         set_task_cpu(migrate_task, this_cpu);
         activate_task(this_rq, migrate_task, 0);
         // del_task->on_rq = TASK_ON_RQ_QUEUED; //CFS
         pulled_task++;
         // check_preempt_curr(this_rq, del_task, 0); //CFS
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