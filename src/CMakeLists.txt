set(QLEVER_PETRIMAPS_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR} ${CMAKE_CURRENT_BINARY_DIR})

include_directories(
	${QLEVER_PETRIMAPS_INCLUDE_DIR}
)

add_subdirectory(util)
add_subdirectory(3rdparty)
add_subdirectory(qlever-petrimaps)
