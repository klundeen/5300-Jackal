#include "storage_engine.h"
using namespace std;

// =====DbBlock=====
// constructor
DbBlock::DbBlock(Dbt &block, BlockID block_id, bool is_new = false) : block(block), block_id(block_id) 
{}

// destructor
DbBlock::~DbBlock() 
{}

void Tree::initialize_new() 
{}






// =====DbFile=====






// =====DbRelation=====