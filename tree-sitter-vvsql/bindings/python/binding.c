#include <Python.h>

typedef struct TSLanguage TSLanguage;

TSLanguage *tree_sitter_vvsql(void);

static PyObject* language(PyObject *self, PyObject *args) {
    return PyLong_FromVoidPtr(tree_sitter_vvsql());
}

static PyMethodDef methods[] = {
    {"language", language, METH_NOARGS,
     "Get the tree-sitter language for vvSQL."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "tree_sitter_vvsql.binding",
    "Tree-sitter bindings for vvSQL",
    -1,
    methods
};

PyMODINIT_FUNC PyInit_binding(void) {
    return PyModule_Create(&module);
}