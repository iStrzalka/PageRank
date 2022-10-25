#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"
#include <array>
#include <cstdio>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unistd.h>

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const
    {
        std::string result;
        char res[65];
        pid_t pid1;
        int fd[2], fd2[2];

        if (pipe2(fd, O_CLOEXEC) < 0) {
            throw std::runtime_error("pipe2() failed!");
        }
        if (pipe2(fd2, O_CLOEXEC) < 0) {
            throw std::runtime_error("pipe2() failed!");
        }
        pid1 = fork();

        if (pid1 == 0) {
            dup2(fd[0], STDIN_FILENO);
            close(fd[0]);
            close(fd[1]);
            close(fd2[0]);

            dup2(fd2[1], STDOUT_FILENO);
            close(fd2[1]);

            execlp("sha256sum", "sha256sum", "-", (char*)NULL);
        } else {

            if (write(fd[1], content.c_str(), content.size()) < 0) {
                throw std::runtime_error("write() failed!");
            }
            close(fd[1]);
            close(fd[0]);

            if (read(fd2[0], res, 65) < 0) {
                throw std::runtime_error("read() failed!");
            }

            close(fd2[0]);
            close(fd2[1]);
        }
        res[64] = '\0';
        result = res;

        return PageId(result);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
