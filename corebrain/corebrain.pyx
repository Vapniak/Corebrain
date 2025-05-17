# distutils: language = c++
cdef public int add(int a, int b):
    return a + b

cdef public char* greet(const char* name):
    return f"Hello, {name.decode('utf-8')}!".encode('utf-8')