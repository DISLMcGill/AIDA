#include <stdio.h>
#include "Python.h"
#include "numpy/npy_common.h"
#include "numpy/ndarrayobject.h"
#include "numpy/arrayobject.h"


static PyObject* convert_func(PyObject *self, PyObject *args){
    int cols,rows,i,j;
    PyObject *ob;
    PyObject *keys;
    PyObject *dict;
    PyObject *arr;
    
    // for the Decimal data type
    PyObject *decimal_mod = PyImport_ImportModule("decimal");
    PyObject *decimal_func = PyObject_GetAttrString(decimal_mod, "Decimal");
    PyObject *one = PyObject_CallFunction(decimal_func, "i", 1);
    PyTypeObject *deciamlType = one->ob_type;

    if (!PyArg_ParseTuple(args, "O!O!ii", &PyList_Type, &ob, &PyList_Type, &keys, &rows, &cols)) {
        PyErr_SetString(PyExc_TypeError, "Input type error.");
        return NULL;
    }

    Py_INCREF(ob);
    Py_INCREF(keys);
    dict = PyDict_New();

    for (i = 0; i < cols; i++){
        // get the key
        PyObject *key = PyList_GetItem(keys, i);
        PyObject *list = PyList_New( (Py_ssize_t)rows );
        
        // get elements
        for (j = 0; j < rows; j++){
            PyObject *row = PyList_GetItem(ob, j);

            PyObject *data = PyDict_GetItem(row, key);
            Py_INCREF(data);
            PyList_SetItem(list, j, data);
        }



        if (!rows){
            arr = PyArray_FROM_OTF(list, NPY_NOTYPE, NPY_IN_ARRAY);
        }else{
            PyObject *first_element = PyList_GetItem(list, 0);
            
            if(PyLong_Check(first_element)){
                arr = PyArray_FROM_OTF(list, NPY_INT64, NPY_IN_ARRAY);
            }else if(PyFloat_Check(first_element)  ||  PyObject_TypeCheck(first_element,deciamlType)){
                arr = PyArray_FROM_OTF(list, NPY_FLOAT64, NPY_IN_ARRAY);
            }else{
                arr = PyArray_FROM_OTF(list, NPY_OBJECT, NPY_IN_ARRAY);
            }
        }

        Py_DECREF(list);
        PyDict_SetItem(dict, key, arr);
    }

    Py_DECREF(ob);
    Py_DECREF(keys);
    Py_DECREF(decimal_mod);
    Py_DECREF(decimal_func);
    Py_DECREF(one);

    return dict;
}

static PyMethodDef convert_funcs[] = {
    {"convert", (PyCFunction)convert_func, METH_VARARGS, NULL},
    { NULL, NULL, 0, NULL }
};

static struct PyModuleDef convert = {
    PyModuleDef_HEAD_INIT,
    "convert", /* name of module */
    NULL,          /* module documentation, may be NULL */
    -1,          /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    convert_funcs
};

PyMODINIT_FUNC PyInit_convert(void){
   import_array();
   return PyModule_Create(&convert);
}
