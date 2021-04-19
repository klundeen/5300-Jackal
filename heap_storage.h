/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#pragma once

#include "db_cxx.h"
#include "storage_engine.h"

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */
class SlottedPage : public DbBlock {
public:

    /**
     * Constructor
     */
    SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);

    // Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
    // but we delete them explicitly just to make sure we don't use them accidentally
    virtual ~SlottedPage() {}

    SlottedPage(const SlottedPage &other) = delete;

    SlottedPage(SlottedPage &&temp) = delete;

    SlottedPage &operator=(const SlottedPage &other) = delete;

    SlottedPage &operator=(SlottedPage &temp) = delete;

    /**
     * Add a new record to the block
     * @param data
     * @return  Return the block id
     */
    virtual RecordID add(const Dbt *data);

    /**
     * Get a recod from the block
     * @param   record_id
     * @return  Return None if the record was deleted
     */
    virtual Dbt *get(RecordID record_id);

    /**
     * Replace the data from the record
     * @param   record_id, data
     */
    virtual void put(RecordID record_id, const Dbt &data);

    /**
     * Delete a record, and reset the size and location to 0
     * @param   record_id
     */
    virtual void del(RecordID record_id);

    /**
     * Find out the non-deleted record ids
     * @return the non-deleted record ids
     */
    virtual RecordIDs *ids(void);

protected:
    u_int16_t num_records;
    u_int16_t end_free;

    /**
     * Get the header block
     * @param size, loc, id
     */
    virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

    /**
     * Put the header block
     * @param id, size, loc
     */
    virtual void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

    /**
     * Calculate the available room to store a record with the size 
     * @param size
     * @return has enough room to store or not
     */
    virtual bool has_room(u_int16_t size);

    
    virtual void slide(u_int16_t start, u_int16_t end);

    virtual u_int16_t get_n(u_int16_t offset);

    virtual void put_n(u_int16_t offset, u_int16_t n);

    virtual void *address(u_int16_t offset);
};

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

    virtual ~HeapFile() {}

    HeapFile(const HeapFile &other) = delete;

    HeapFile(HeapFile &&temp) = delete;

    HeapFile &operator=(const HeapFile &other) = delete;

    HeapFile &operator=(HeapFile &&temp) = delete;


    /**
     * Create the physical file
     */
    virtual void create(void);

    /**
     * Drop the physical file
     */
    virtual void drop(void);

    /**
     * Open the physical file
     */
    virtual void open(void);

    /**
     * Close the physical file
     */
    virtual void close(void);

    /**
     * Allocate a new block for the db file
     * @return the empty block
     */
    virtual SlottedPage *get_new(void);

    /**
     * Get the block by given block_id in file
     * @param block_id
     * @return the block
     */
    virtual SlottedPage *get(BlockID block_id);

    /**
     * Put a block into the db file
     * @param block
     */
    virtual void put(DbBlock *block);

    /**
     * Find existing block ids
     * @return a sequence of existing block ids
     */
    virtual BlockIDs *block_ids();

    virtual u_int32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename;
    u_int32_t last;
    bool closed;
    Db db;

    virtual void db_open(uint flags = 0);
};

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable &other) = delete;

    HeapTable(HeapTable &&temp) = delete;

    HeapTable &operator=(const HeapTable &other) = delete;

    HeapTable &operator=(HeapTable &&temp) = delete;

    /**
     * Execute CREATE TABLLE statement
     */
    virtual void create();

    /**
     * Execute CREATE TABLLE IF NOT EXISTS statement
     */
    virtual void create_if_not_exists();

    /**
     * Execute DROP TABLE statement
     */
    virtual void drop();

    /**
     * Open the existing table
     */
    virtual void open();

    /**
     * Close the existing table
     */
    virtual void close();

    /**
     * Execute INSERT INTO statement
     * @param row
     * @return the handle for insert row
     */
    virtual Handle insert(const ValueDict *row);

    // "Not milestone 2 for HeapTable"  
    virtual void update(const Handle handle, const ValueDict *new_values);

    // "Not milestone 2 for HeapTable"
    virtual void del(const Handle handle);

    // "Not milestone 2 for HeapTable"
    virtual Handles *select();

    virtual Handles *select(const ValueDict *where);

    // "Not milestone 2 for HeapTable"
    virtual ValueDict *project(Handle handle);

    // "Not milestone 2 for HeapTable"
    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

protected:
    HeapFile file;

    /**
     * To validate the given row can be inserted
     * @param row
     * @return the full row
     */
    virtual ValueDict *validate(const ValueDict *row);

    /**
     * To add a record to the file
     * @param row
     * @return the last block id in record and the record id
     */
    virtual Handle append(const ValueDict *row);

    // return the bits to go into the file
    // caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
    virtual Dbt *marshal(const ValueDict *row);

    
    virtual ValueDict *unmarshal(Dbt *data);
};

bool test_heap_storage();