#include "db.h"

void serialize_row(Row* source, void* destination) {
    memcpy(destination + 0, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        
        if (page_num <= num_pages) {
            lseek(pager->file_desciptor, page_num * PAGE_SIZE, SEEK_SET);
            size_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page
    }

    return pager->pages[page_num];
}

void* cursor_value(Cursor* cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;

    return page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_row) {
        cursor->end_of_table = true;
    }
}

Pager* pager_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT | S_IWUSR | S_IRUSR);
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_desciptor = fd;
    Pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    int offset = lseek(pager->file_desciptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offest == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    size_t bytes_written = write(pager->file_desciptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_row == 0);

    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor =  malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_row;
    cursor->end_of_table = true;

    return cursor;
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    Table* table = malloc(sizeof(Table));
    table->num_row = pager->file_length / ROW_SIZE;
    table->pager = pager;
    return table;
}

void db_close(Table* table) {
    Pager8 pager = table->pager;
    uint32_t num_full_pages = table->num_row / ROWS_PER_PAGE;
    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);

}

void free_table(Table* table) {
    for (int i = 0; table->pages[i]; i++) {
        free(table->pages[i]);
    }
    free(table);
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int args_assigned = sscanf(
            input_buffer->buffer,
            "Insert %d %s %s",
            &(statement->row_to_insert.id),
            statement->row_to_insert.username,
            statement->row_to_insert.email
        );
        if (args_assigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select")) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }   
    return PREPARE_UNRECONGIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_row >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Cursor* cursor = table_end(table);
    serialize_row(&statement->row_to_insert, cursor_value(cursor));
    table->num_row += 1;

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);

    Row row;
    while(!(cursor->end_of_table)) {
        deserialize_row(cursor_value(table), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);

    return EXECUTE_SUCCESS;
}

void execute_statement(Statement* statement, Table* table) {
    switch(statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}