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
  TARGETS petrimaps DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
              GROUP_READ GROUP_WRITE GROUP_EXECUTE
              WORLD_READ WORLD_EXECUTE
)

###############################################################
# CPack packaging
###############################################################

set(CPACK_PACKAGE_NAME "qlever-petrimaps")
set(CPACK_PACKAGE_VERSION "0.5.45")

# Debian package revision number (e.g., the "1" in "0.5.42-1~plucky~25.04").
# Override with: cmake -DPACKAGE_REVISION=2
set(PACKAGE_REVISION "1" CACHE STRING "Debian package revision number")

# Version suffix for distribution-specific packages (e.g., "~plucky~25.04").
# Auto-detected from /etc/os-release for Ubuntu and Debian.
# For derived distros, override with: cmake -DVERSION_SUFFIX="~noble~24.04"
set(DEFAULT_VERSION_SUFFIX "")
if (EXISTS "/etc/os-release")
    file(STRINGS "/etc/os-release" OS_RELEASE_CONTENTS)

    set(_OS_ID "")
    set(_OS_VERSION_ID "")
    set(_OS_VERSION_CODENAME "")

    foreach(line ${OS_RELEASE_CONTENTS})
        if (line MATCHES "^ID=\"?([^\"]*)\"?$")
            set(_OS_ID "${CMAKE_MATCH_1}")
        elseif (line MATCHES "^VERSION_ID=\"?([^\"]*)\"?$")
            set(_OS_VERSION_ID "${CMAKE_MATCH_1}")
        elseif (line MATCHES "^VERSION_CODENAME=\"?([^\"]*)\"?$")
            set(_OS_VERSION_CODENAME "${CMAKE_MATCH_1}")
        endif()
    endforeach()

    if (_OS_VERSION_CODENAME AND _OS_VERSION_ID)
        if (_OS_ID STREQUAL "ubuntu")
            set(DEFAULT_VERSION_SUFFIX "~${_OS_VERSION_CODENAME}~${_OS_VERSION_ID}")
        elseif (_OS_ID STREQUAL "debian")
            set(DEFAULT_VERSION_SUFFIX "~${_OS_VERSION_CODENAME}~deb${_OS_VERSION_ID}")
        endif()
    endif()
endif()

set(VERSION_SUFFIX "${DEFAULT_VERSION_SUFFIX}" CACHE STRING "Distribution-specific version suffix")

set(CPACK_PACKAGE_VERSION "${CPACK_PACKAGE_VERSION}-${PACKAGE_REVISION}${VERSION_SUFFIX}")

set(CPACK_PACKAGE_CONTACT "mundhahj@tf.uni-freiburg.de")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/ad-freiburg/qlever-petrimaps")

set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/packages")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)

set(CPACK_VERBATIM_VARIABLES YES)

include(CPack)
