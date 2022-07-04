aux_source_directory(utils/ UTILS_LIST)
aux_source_directory(memory/ MEMORY_LIST)
aux_source_directory(db/ DB_LIST)

add_library(sensekv SHARED ${MEMORY_LIST} ${UTILS_LIST} ${DB_LIST})