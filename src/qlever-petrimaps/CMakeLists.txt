file(GLOB_RECURSE QLEVER_PETRIMAPS_SRC *.cpp)

set(qlever_petrimaps_main PetriMapsMain.cpp)

list(REMOVE_ITEM QLEVER_PETRIMAPS_SRC ${qlever_petrimaps_main})

include_directories(
	${QLEVER_PETRIMAPS_INCLUDE_DIR}
)

add_executable(petrimaps ${qlever_petrimaps_main})
add_library(qlever_petrimaps_dep ${QLEVER_PETRIMAPS_SRC})

add_custom_command(
	OUTPUT index.h
	COMMAND cd ${PROJECT_BINARY_DIR}/../web/ && xxd -i index.html > ${CMAKE_CURRENT_BINARY_DIR}/index.h
	DEPENDS "${PROJECT_BINARY_DIR}/../web/index.html"
	VERBATIM
)

add_custom_command(
	OUTPUT build.h
	COMMAND cd ${PROJECT_BINARY_DIR}/../web/ && cat leaflet.js NonTiledLayer.js script.js > merged.js && java -jar closurec/compiler.jar -O SIMPLE leaflet.js NonTiledLayer.js script.js > build.js && xxd -i build.js > ${CMAKE_CURRENT_BINARY_DIR}/build.h && rm build.js
	DEPENDS "${PROJECT_BINARY_DIR}/../web/script.js"
	VERBATIM
)

add_custom_target(htmlfiles DEPENDS index.h build.h)

add_dependencies(qlever_petrimaps_dep htmlfiles)

target_link_libraries(petrimaps qlever_petrimaps_dep 3rdparty_dep util -lpthread -lcurl -lpng)
