file(GLOB_RECURSE QLEVER_PETRIMAPS_SRC *.cpp)
find_package(PNG REQUIRED)


set(qlever_petrimaps_main PetriMapsMain.cpp)

list(REMOVE_ITEM QLEVER_PETRIMAPS_SRC ${qlever_petrimaps_main})

include_directories(
	${QLEVER_PETRIMAPS_INCLUDE_DIR}
	${PNG_INCLUDE_DIRS}_
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
	OUTPUT style.h
	COMMAND cd ${PROJECT_BINARY_DIR}/../web/ && sed -e "s/^\\s*//g" -e "s/\\s\\+/ /g" style.css | tr -d '\\n' > build.css && xxd -i build.css > ${CMAKE_CURRENT_BINARY_DIR}/style.h
	DEPENDS "${PROJECT_BINARY_DIR}/../web/style.css"
	VERBATIM
)

add_custom_command(
	OUTPUT build.h
	COMMAND cd ${PROJECT_BINARY_DIR}/../web/ && java -jar closurec/compiler.jar -W QUIET -O SIMPLE leaflet.js NonTiledLayer.js script.js > build.js && xxd -i build.js > ${CMAKE_CURRENT_BINARY_DIR}/build.h && rm build.js
	DEPENDS "${PROJECT_BINARY_DIR}/../web/script.js" "${PROJECT_BINARY_DIR}/../web/leaflet.js" "${PROJECT_BINARY_DIR}/../web/leaflet-heat.js"
	VERBATIM
)

add_custom_target(htmlfiles DEPENDS index.h build.h style.h)

add_dependencies(qlever_petrimaps_dep htmlfiles)

target_link_libraries(petrimaps qlever_petrimaps_dep 3rdparty_dep util ${PNG_LIBRARIES} -lpthread -lcurl)
