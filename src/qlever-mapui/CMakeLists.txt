file(GLOB_RECURSE QLEVER_MAPUI_SRC *.cpp)

set(qlever_mapui_main MapUIMain.cpp)

list(REMOVE_ITEM QLEVER_MAPUI_SRC ${qlever_mapui_main})

include_directories(
	${QLEVER_MAPUI_INCLUDE_DIR}
)

add_executable(mapui ${qlever_mapui_main})
add_library(qlever_mapui_dep ${QLEVER_MAPUI_SRC})

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

add_dependencies(qlever_mapui_dep htmlfiles)

target_link_libraries(mapui qlever_mapui_dep 3rdparty_dep util -lpthread -lcurl -lpng)
