add_executable(qleverPetrimapsTest TestMain.cpp)
target_link_libraries(qleverPetrimapsTest qlever_petrimaps_dep 3rdparty_dep pb_util pb_util_geo pb_util_json pb_util_http ${PNG_LIBRARIES} -lpthread -lcurl)

add_test(NAME qleverPetrimapsTest COMMAND qleverPetrimapsTest)
