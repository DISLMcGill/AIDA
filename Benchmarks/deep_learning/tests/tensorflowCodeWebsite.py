from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);

def training(dw):


    def rmse(y, y_hat):
        """Compute root mean squared error"""
        return tf.sqrt(tf.reduce_mean(tf.square((y - y_hat))))

    def forward(x, e):
        """Forward pass for our fuction"""
        # tensorflow has automatic broadcasting
        # so we do not need to reshape e manually
        return tf.pow(x, e)

    n = 100  # number of examples
    learning_rate = 5e-6

    # Placeholders for data
    x = tf.placeholder(tf.float32)
    y = tf.placeholder(tf.float32)

    # Model parameters
    exp = tf.constant(2.0)
    exp_hat = tf.Variable(4.0, name='exp_hat')

    # Model definition
    y_hat = forward(x, exp_hat)

    # Optimizer
    loss = rmse(y, y_hat)
    opt = tf.train.GradientDescentOptimizer(learning_rate)

    # We will run this operation to perform a single training step,
    # e.g. opt.step() in Pytorch.
    # Execution of this operation will also update model parameters
    train_op = opt.minimize(loss)

    # Let's generate some training data
    x_train = np.random.rand(n) + 10
    y_train = x_train ** 2

    loss_history = []
    exp_history = []

    # First, we need to create a Tensorflow session object
    with tf.Session() as sess:
        # Initialize all defined variables
        tf.global_variables_initializer().run()

        # Training loop
        for i in range(0, 500):
            print("Iteration %d" % i)
            # Run a single trainig step
            curr_loss, curr_exp, _ = sess.run([loss, exp_hat, train_op], feed_dict={x: x_train, y: y_train})

            print("loss = %s" % curr_loss)
            print("exp = %s" % curr_exp)

            # Do some recordings for plots
            loss_history.append(curr_loss)
            exp_history.append(curr_exp)
    return exp_history[-1]
exp = dw._X(training)
print(exp)