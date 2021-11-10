from aida.aida import *;

host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);

def Test(dw):
    const1 = tf.constant([[1,2,3], [1,2,3]]);
    const2 = tf.constant([[3,4,5], [3,4,5]]);

    result = tf.add(const1, const2);

    return result

result = dw._X(Test)
print(result)