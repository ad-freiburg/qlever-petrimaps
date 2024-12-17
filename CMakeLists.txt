cmake_minimum_required (VERSION 3.5)
set(CMAKE_CXX_STANDARD 14)

project (qlever-petrimaps)

# only change first char
if (CMAKE_BUILD_TYPE)
	string(SUBSTRING ${CMAKE_BUILD_TYPE} 0 1 FIRST_CHAR)
	string(TOUPPER ${FIRST_CHAR} FIRST_CHAR)
	string(REGEX REPLACE "^.(.*)" "${FIRST_CHAR}\\1" CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
endif()

# old solution
#if (CMAKE_BUILD_TYPE)
#	string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
#endif()

enable_testing()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}")


find_package(OpenMP)

# set compiler flags, see http://stackoverflow.com/questions/7724569/debug-vs-release-in-cmake
set(CMAKE_CXX_FLAGS "-march=native -O3 -Wall -Wno-format-extra-args -Wextra -Wformat-nonliteral -Wformat-security -Wformat=2 -Wextra -Wno-implicit-fallthrough -Wno-narrowing -pedantic")
set(CMAKE_C_FLAGS "-march=native -O3 -Wall -Wno-format-extra-args -Wextra -Wformat-nonliteral -Wformat-security -Wformat=2 -Wextra -Wno-implicit-fallthrough -Wno-narrowing -pedantic")

if (OPENMP_FOUND)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

if (PNG_FOUND)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

set(CMAKE_CXX_FLAGS_DEBUG          "-Og -g -DLOGLEVEL=3")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "${CMAKE_CXX_FLAGS} -DLOGLEVEL=2")
set(CMAKE_CXX_FLAGS_RELEASE        "${CMAKE_CXX_FLAGS} -DLOGLEVEL=2")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS} -g -DLOGLEVEL=3")

# export compile commands to tools like clang
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# http://brianmilco.blogspot.de/2012/11/cmake-automatically-use-git-tags-as.html
include(GetGitRevisionDescription)
git_get_tag(VERSION_GIT)
get_git_is_dirty(VERSION_GIT_IS_DIRTY)
if ("${VERSION_GIT_IS_DIRTY}" STREQUAL "")
	set(VERSION_GIT_FULL "${VERSION_GIT}")
else()
	set(VERSION_GIT_FULL "${VERSION_GIT}-${VERSION_GIT_IS_DIRTY}")
endif()

add_subdirectory(src)

# install target
install(
  FILES ${CMAKE_BINARY_DIR}/petrimaps DESTINATION bin
  PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE
)
