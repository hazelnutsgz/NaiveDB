#include "config.h"

struct Row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
};
typedef struct Row_t Row;

struct Pager_t {
    int file_desciptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
    Pager* pager;
    uint32_t root_page_num;
};
typedef struct Table_t Table;

struct Cursor_t {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
};
typedef struct Cursor_t Cursor;

struct Statement_t {
    StatementType type;
    Row row_to_insert;
};
typedef struct Statement_t Statement;

void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);

void* get_page(Pager* pager, uint32_t page_num);
Pager* pager_open(const char* filename);
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size);

Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void free_table(Table* table);


Table* db_open(const char* filename);
void db_close(Table* table);


void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);
