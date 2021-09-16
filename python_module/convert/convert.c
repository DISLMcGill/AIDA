#include "Python.h"
#include "numpy/npy_common.h"
#include "numpy/ndarrayobject.h"
#include "numpy/arrayobject.h"

static int init_numpy(void){
    return _import_array(); 
}

static PyObject* convert_func(PyObject *self, PyObject *args){
    /* This function converts a row-based data set to a column-based one.
     *
     *   It takes 4 inputs:
     *       1. A list of dictionaries that each one contains data for a row.
     *       2. A list of column names
     *       3. The number of rows
     *       4. The number of columns
    */
    int cols,rows,i,j;
    PyObject *ob;
    PyObject *keys;
    PyObject *dict;
    PyObject *arr;
    PyObject *col_lists;

    // python decimal class
    PyObject * decimal_mod = PyImport_ImportModule("decimal");
    PyObject * decimal_cls = PyObject_GetAttrString(decimal_mod, "Decimal");


    // init numpy
    int not_init_numpy =  init_numpy();

    if(not_init_numpy){
        // numpy is not initiated
    }

    // verify if the input objects have correct types.
    if (!PyArg_ParseTuple(args, "O!O!ii", &PyList_Type, &ob, &PyList_Type, &keys, &rows, &cols)) {
        PyErr_SetString(PyExc_TypeError, "Input type error.");
        return NULL;
    }

    Py_INCREF(ob);
    Py_INCREF(keys);
    dict = PyDict_New();





    col_lists = PyDict_New();

    for (i = 0; i < cols; i++){
        PyObject *col_list = PyList_New( (Py_ssize_t)rows );
        PyObject *key = PyList_GetItem(keys, i);
        PyDict_SetItem(col_lists, key, col_list);
    }

    for (j = 0; j < rows; j++){
        PyObject *row = PyList_GetItem(ob, j);
        for (i = 0; i < cols; i++){
            PyObject *key = PyList_GetItem(keys, i);
	    Py_INCREF(key);
            PyObject *col_list = PyDict_GetItem(col_lists, key);
            PyObject *data = PyDict_GetItem(row, key);
	    Py_DECREF(key);
            Py_INCREF(data);
            PyList_SetItem(col_list, j, data);
        }
    }

    for (i = 0; i < cols; i++){
        PyObject *key = PyList_GetItem(keys, i);
        PyObject *col_list = PyDict_GetItem(col_lists, key);

        if (!rows){
            arr = PyArray_FROM_OTF(col_list, NPY_NOTYPE, NPY_IN_ARRAY);
        }else{
            PyObject *first_element = PyList_GetItem(col_list, 0);
            if(PyLong_Check(first_element)){
                arr = PyArray_FROM_OTF(col_list, NPY_INT64, NPY_IN_ARRAY);
            }else if(PyFloat_Check(first_element) || PyObject_IsInstance(first_element,decimal_cls)){
                arr = PyArray_FROM_OTF(col_list, NPY_FLOAT64, NPY_IN_ARRAY);
            }else{
                arr = PyArray_FROM_OTF(col_list, NPY_OBJECT, NPY_IN_ARRAY);
            }
        }

        Py_DECREF(col_list);
        PyDict_SetItem(dict, key, arr);
    }
    
       

    Py_DECREF(ob);
    Py_DECREF(keys);
    Py_DECREF(col_lists);
    Py_DECREF(decimal_mod);
    Py_DECREF(decimal_cls);

    return dict;
}

static PyMethodDef convert_funcs[] = {
    {"convert", (PyCFunction)convert_func, METH_VARARGS, NULL},
    { NULL, NULL, 0, NULL }
};

static struct PyModuleDef convert = {
    PyModuleDef_HEAD_INIT,
    "convert",    /* name of module */
    NULL,         /* module documentation, may be NULL */
    -1,           /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
    convert_funcs
};

PyMODINIT_FUNC PyInit_convert(void){
   import_array();
   return PyModule_Create(&convert);
}
