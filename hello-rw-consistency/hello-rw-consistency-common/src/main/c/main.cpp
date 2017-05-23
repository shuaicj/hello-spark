#include <cstdio>
#include <cstring>
#include <fstream>
#include "md5.h"

#define MD5_RESULT_LEN    16
#define BUF_LEN           4096

int md5_c(const char* file);
int md5_cpp(const char* file);
int usage(const char* cmd);

int main(int argc, char** argv) {
    if (argc != 3) { 
        return usage(argv[0]);
    } 
    if (strcmp("-c", argv[1]) == 0) { 
        return md5_c(argv[2]);
    }
    if (strcmp("-cpp", argv[1]) == 0) { 
        return md5_cpp(argv[2]);
    }
    return usage(argv[0]);
}

int md5_c(const char* file) {
    FILE* fp = fopen(file, "rb");
    if (fp) {
        MD5_CTX ctx;
        MD5_Init(&ctx);
        char buf[BUF_LEN] = {0};
        int num = 0;
        while ((num = fread(buf, sizeof(char), BUF_LEN, fp)) != 0) {
            MD5_Update(&ctx, buf, num);
        }
        fclose(fp);

        unsigned char result[MD5_RESULT_LEN];
        MD5_Final(result, &ctx);
        for(int i = 0; i < MD5_RESULT_LEN; i++) {
            printf("%02x", result[i]);
        }
        printf("\n");
        return 0;
    }
    return 1;
}

int md5_cpp(const char* file) {
    std::ifstream f(file, std::ifstream::binary);
    if (f) {
        MD5_CTX ctx;
        MD5_Init(&ctx);
        char buf[BUF_LEN] = {0};
        int num = 0;
        while (!f.eof() && f.good()) {
            f.read(buf, BUF_LEN);
            MD5_Update(&ctx, buf, f.gcount());
        }
        f.close();

        unsigned char result[MD5_RESULT_LEN];
        MD5_Final(result, &ctx);
        for(int i = 0; i < MD5_RESULT_LEN; i++) {
            printf("%02x", result[i]);
        }
        printf("\n");
        return 0;
    }
    return 1;
}

int usage(const char* cmd) {
    printf("usage:\n"); 
    printf("  %s -c <file>\n", cmd); 
    printf("  %s -cpp <file>\n", cmd); 
    return 1;
}
