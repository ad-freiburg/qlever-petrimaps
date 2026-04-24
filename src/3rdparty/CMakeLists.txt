file(GLOB_RECURSE 3RDPARTY_SRC *.c)

add_library(3rdparty_dep ${3RDPARTY_SRC})

target_include_directories(3rdparty_dep PUBLIC
	$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
