

enum MetaCommandResult_t{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECONGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
    PREPARE_SUCCESS, 
    PREPARE_UNRECONGIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t { 
    STATEMENT_INSERT, 
    STATEMENT_SELECT 
};
typedef enum StatementType_t StatementType;

enum ExecuteResult_t {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
};
typedef enum ExecuteResult_t ExecuteResult;

struct InputBuffer_t {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
};
typedef struct InputBuffer_t InputBuffer;


InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer* input_buffer);
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table);