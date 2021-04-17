# 5300-Jackal

## Milestone 1: Skeleton -- Team: Tong Xu, Yinhui Li

On cs1, clone the repo and build sql5300 by using Makefile
```
$ cd ~/cpsc5300
$ git clone https://github.com/klundeen/5300-Jackal.git
$ cd 5300-Jackal
$ make
```
**Remember to have [Berkeley DB data](https://seattleu.instructure.com/courses/1597073/pages/getting-set-up-on-cs1?module_item_id=17258588) in your directory on cs1**
```
$ ./sql5300 ~/cpsc5300/data
```
---

## Milestone 2: Rudimentary Storage Engine -- Team: Tong Xu, Yinhui Li
In heap_storage, HeapTable did not implement the following functions because it is not required for milestone 2.
* virtual void update(const Handle handle, const ValueDict *new_values);
* virtual void del(const Handle handle);
* virtual Handles *select();
* virtual ValueDict *project(Handle handle);
* virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

