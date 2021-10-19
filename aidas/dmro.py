import sys;
import types;

import logging;

import threading;

# TODO: Investigate __getattribute__ , __getattr__ , __setattr__ , __delete__ to see if those methods
# TODO: combined with a way to keep track of users can be used to implement privacy.
class DMROrepository(types.ModuleType):

    #Keep track of all repositories created.
    __DMROmoduleList = {};
    _lock = __ClassLock = threading.RLock();
    #Parent module to store all user created repositories.
    __usrdmroreps = None;
    (USERDMROREPS) = ('usrdmroreps');

    # We definitely need a module name for argument
    def __init__(self, name, *args, **kwargs):

        logging.debug("create dmro repository request for {}".format(name));
        #Ensures only one instance is in the process of creation at any given time.
        with self.__ClassLock:
          # Ensure no name conflicts occur.
          if(self.__DMROmoduleList.get(name) is not None):
              logging.warning("Error DMRO for {} already exists".format(name));
              raise AttributeError("Error DMRO {} already exists.".format(name));
          if(hasattr(sys.modules[__name__], name)):
              logging.warning("Error a module by name {} already exists".format(name));
              raise AttributeError("Error a module by name {} already exists.".format(name));

          # If the parent user repository module does not exist yet, create one.
          if(self.__usrdmroreps is None):
              self.__class__.__usrdmroreps = types.ModuleType(self.USERDMROREPS);
              sys.modules['dmro.{}'.format(self.USERDMROREPS)] = self.__usrdmroreps;
              setattr(sys.modules['aidas.dmro'], self.USERDMROREPS, self.__usrdmroreps);

          # Make the dmro and the parent user repository modules visible in the main
          if(not hasattr(sys.modules['__main__'], 'dmro')):
              setattr(sys.modules['__main__'], 'dmro', sys.modules['aidas.dmro']);
          if(not hasattr(sys.modules['__main__'], 'dmro.{}'.format(self.USERDMROREPS))):
              setattr(sys.modules['__main__'], 'dmro.{}'.format(self.USERDMROREPS), self.__usrdmroreps);

          # Create the new user repository module
          super().__init__(name, *args, **kwargs);
          logging.debug("created dmro for {}".format(name));
          # Make sure to undo all of this in rmDMROrepository.
          self.__DMROmoduleList[name] = self;
          # Add it to the parent user repository
          setattr(self.__usrdmroreps, name, self);
          sys.modules['dmro.{}.{}'.format(self.USERDMROREPS, name)] = self;
          sys.modules[name] = self;
          #Make the new user DMRO repository visible in the calling module;
          #exec('from dmro.{} import {};'.format(self.USERDMROREPS, name),sys.modules[ inspect.stack()[1][0].f_locals['__name__'] ].__dict__);

          self._lock = threading.RLock();

    def __getattribute__(self, item):
        """ Retrieve an object from the DMRO """
        # TODO Add security feature ?
        with super().__getattribute__('_lock'):
            return super().__getattribute__(item);

    def __setattr__(self, key, value):
        """ Add an object to the DMRO """
        # TODO Add security feature ?
        with super().__getattribute__('_lock'):
          try:
            self.__getattribute__(key);
            if(key in {'_lock'}):
                super().__setattr__(key, value);
                return;

          except AttributeError:
            super().__setattr__(key, value);
            # Increment the reference count of the object being added.
            if(hasattr(value, '__addDMRO__')):
                getattr(value, '__addDMRO__')();
            return;

          # Raise an exception if the attribute name already exists.
          raise AttributeError("Error module {} already contains object {}.".format(self ,key));


    def __delattr__(self, item):
        """ Remove an object from the DMRO """
        # TODO Add security feature ?
        with super().__getattribute__('_lock'):
          try:
              obj=self.__getattribute__(item);
              super().__delattr__(item);

              # Decrement the reference count of the object being removed.
              if(hasattr(obj, '__rmDMRO__')):
                  getattr(obj, '__rmDMRO__')();
              return;
          except AttributeError:
              pass;

          # Raise an exception if the attribute name do not exist.
          raise AttributeError("Error module {} does not contain object {}".format(self, item));

    @property
    def lock(self):
        return self._lock;

    @classmethod
    def rmDMROrepository(cls, dmrorep):
        """ Remove a DMRO repository """

        with cls.__ClassLock:
          # Nothing to do here.
          if(cls.__usrdmroreps is None or dmrorep is None):
              return;

          # TODO , should it be allowed to pass the name of DMRO repository instead of the module reference ?
          # Should pass only objects of this class.
          if(not isinstance(dmrorep, cls)):
              raise TypeError("Error module {} is not of type {}".format(dmrorep.__name__, cls.__name__));

          # We probably already removed it ? Nothing to do here.
          # This shouldn't happen ? Raise an error ?
          if(cls.__DMROmoduleList.get(dmrorep.__name__) is None):
              return;

          # Undo all the work done in __init__
          del cls.__DMROmoduleList[dmrorep.__name__];
          delattr(cls.__usrdmroreps, dmrorep.__name__);
          del sys.modules['dmro.{}.{}'.format(cls.USERDMROREPS, dmrorep.__name__)];
          del sys.modules[dmrorep.__name__];

    @classmethod
    def getDMROrepos(cls):
        """ Return the list of DMRO repositories currently present. """
        with cls.__ClassLock:
            return cls.__DMROmoduleList.copy();

