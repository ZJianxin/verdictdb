'''
    Copyright 2018 University of Michigan

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
'''
import atexit
import importlib
import os
from py4j.java_gateway import JavaGateway
import pkg_resources
import sys
from .verdictresult import SingleResultSet
from . import verdictcommon
from .verdictexception import *
from time import sleep, time


class Py4jReloader:
    '''
    To load py4j from the original system path (not from spark's lib folder)
    Note: pyspark seems to create a temp directory for py4j, which is no longer
          accessible after its initialization is done. However, the name 'py4j'
          still hides the system py4j.
    '''
    def __init__(self):
        original_sys_path = sys.path
        no_spark_sys_path = []
        for p in original_sys_path:
            if 'spark' not in p:
                no_spark_sys_path.append(p)
        sys.path = no_spark_sys_path    # change temporarily
        importlib.reload(py4j)
        self.reload_modules_in(py4j)
        self._java_gateway = py4j.java_gateway.JavaGateway
        # sys.path = original_sys_path    # restore sys.path
        # importlib.reload(py4j)          # restore py4j

    def reload_modules_in(self, package):
        from types import ModuleType
        modules_to_import = []
        for name in dir(package):
            submodule = getattr(package, name)
            if isinstance(submodule, ModuleType):
                # print(submodule)
                modules_to_import.append(submodule)

        # the import must follow a certain order; however, we do not know that.
        # thus, we try until all modules are imported
        while len(modules_to_import) > 0:
            submodule = modules_to_import.pop(0)
            try:
                importlib.reload(submodule)
            except ImportError:
                modules_to_import.append(submodule)

    def get_java_gateway(self):
        return self._java_gateway

# py4j_reloader = Py4jReloader()


# To properly close all connections
created_verdict_contexts = []

def close_verdict_contexts():
    for c in created_verdict_contexts:
        c.close()

atexit.register(close_verdict_contexts)


class VerdictContext:
    """
    The main Python interface to VerdictDB's Java core.

    All necessary JDBC drivers (i.e., jar files) are already included in the
    pyverdict package. These drivers are included in the classpath.

    JVM is started on a separate process, which is to prevent KeyboardInterrupt
    (or Ctrl+C) from killing the JVM process.

    Args:
        url: jdbc connection string
        extra_class_path: The extra classpath used in addition to verdictdb's
                          jar file. This arg can either be a single str or a
                          list of str; each str is an absolute path
                          to a jar file.
        spark_session: an instance of pyspark.sql.session
    """

    def __init__(
        self,
        url=None,
        extra_class_path=None,
        user=None,
        password=None,
        verdictdbmetaschema=None,
        verdictdbtempschema=None,
        spark_session=None,
    ):
        if url is not None:
            self._gateway = self._get_gateway(extra_class_path)
            self._context = self._get_context(
                self._gateway,
                url,
                user,
                password,
                verdictdbmetaschema,
                verdictdbtempschema,
            )

            self._dbtype = self._get_dbtype(url)
            self._url = url
            self.is_closed = False
        if spark_session is not None:
            assert str(type(spark_session)) \
                == "<class 'pyspark.sql.session.SparkSession'>"
            self._spark_session = spark_session
            self._jspark_session = spark_session._jsparkSession
            self._spark_context = spark_session._sc
            self._gateway = self._get_pyspark_gateway(spark_session)
            self._context = self._load_or_get_spark_context(
                                self._gateway, self._jspark_session)
            self._dbtype = 'spark'
            self.is_closed = False
        self.version = self._get_verdictdb_version()

    def close(self):
        if not self.is_closed:
            self._context.close()
            self._gateway.close()

            self.is_closed = True

        return

    @classmethod
    def new_spark_context(cls, spark):
        ins = cls(spark_session=spark)
        created_verdict_contexts.append(ins)
        return ins


#    def new_mysql_context(cls, host, user, password=None, port=3306):
    @classmethod
    def new_mysql_context(
        cls,
        host,
        user,
        password=None,
        port=3306,
        verdictdbmetaschema=None,
        verdictdbtempschema=None,
    ):

        if password is None:
            connection_string = \
                f'jdbc:mysql://{host}:{port}?user={user}'
        else:
            connection_string = \
                f'jdbc:mysql://{host}:{port}?user={user}&password={password}'

        ins = cls(
            connection_string,
            verdictdbmetaschema=verdictdbmetaschema,
            verdictdbtempschema=verdictdbtempschema,
        )

        created_verdict_contexts.append(ins)
        return ins

    @classmethod
    def new_presto_context(
        cls,
        host,
        catalog,
        user,
        password=None,
        port=8080,
        verdictdbmetaschema=None,
        verdictdbtempschema=None,
    ):
        if password is None:
            connection_string = \
                f'jdbc:presto://{host}:{port}/{catalog}?user={user}'
        else:
            connection_string = \
                f'jdbc:presto://{host}:{port}/{catalog}?' \
                f'user={user}&password={password}'

        ins = cls(
            connection_string,
            verdictdbmetaschema=verdictdbmetaschema,
            verdictdbtempschema=verdictdbtempschema,
        )

        created_verdict_contexts.append(ins)
        return ins

    @classmethod
    def new_redshift_context(cls, host, port, dbname='', user=None, password=None):
        pre_connection_string = 'jdbc:redshift://%s:%s%s'

        dbname_str = ''
        if len(dbname) > 0:
            dbname_str = '/%s' % dbname

        connection_string = pre_connection_string % (host, str(port), dbname_str)

        instance = cls(connection_string, user=user, password=password)
        created_verdict_contexts.append(instance)

        return instance

    @classmethod
    def new_impala_context(
        cls,
        host,
        port,
        schema=None,
        username=None,
        password=None,
        verdictdbmetaschema=None,
        verdictdbtempschema=None,
    ):
        connection_string = 'jdbc:impala://%s:%s%s%s'

        schema_str = ''
        if schema is not None:
            schema_str = '/%s' % schema

        username_str = ''
        if username is not None:
            username_str = 'UID=%s;' % username
        password_str = ''
        if password is not None:
            password_str = 'PWD=%s;' % password

        pre_params_str = '%s%s' % (username_str, password_str)

        params_str = ''
        if len(pre_params_str) > 0:
            params_str = ';%s' % pre_params_str

        instance = cls(
            connection_string % (host, str(port), schema_str, params_str),
            verdictdbmetaschema=verdictdbmetaschema,
            verdictdbtempschema=verdictdbtempschema,
        )

        created_verdict_contexts.append(instance)

        return instance


    @classmethod
    def new_postgres_context(
        cls,
        dbname,
        user,
        password=None,
        host='localhost',
        port=5432,
    ):

        passwordStr = ''
        if password is not None:
            passwordStr = '&password=%s' % password

        connection_string = 'jdbc:postgresql://%s:%s/%s?user=%s%s' % (
            host,
            port,
            dbname,
            user,
            passwordStr
        )

        ins = cls(connection_string)
        created_verdict_contexts.append(ins)
        return ins

    def set_loglevel(self, level):
        self._context.setLoglevel(level)

    def set_log_level(self, level):
        self.set_loglevel(level)

    def sql(self, query):
        return self.sql_raw_result(query).to_df()

    def sql_raw_result(self, query):
        '''
        Development API
        '''
        start_time = time()

        java_resultset = self._context.sql(query)
        if java_resultset is None:
            msg = 'processed'
            result_set = SingleResultSet.status_result(msg, self)
        else:
            result_set = SingleResultSet.from_java_resultset(java_resultset, self)

        elapsed_time = time() - start_time
        if elapsed_time < 60.0:
            elapsed_time_str = "{0:.3f} seconds".format(elapsed_time)
        else:
            elapsed_min = elapsed_time // 60
            elapsed_sec = elapsed_time % 60
            elapsed_time_str = "{0} mins {1:.3f} seconds".format(
                                   elapsed_min, elapsed_sec)
        print("{} row(s) in the result ({})".format(
                  len(result_set.rows()), elapsed_time_str))

        return result_set

    def get_dbtype(self):
        return self._dbtype.lower()

    def _get_dbtype(self, url):
        tokenized_url = url.split(':')
        if tokenized_url[0] != 'jdbc':
            raise VerdictException('The url must start with \'jdbc\'')
        if len(tokenized_url) < 2:
            raise VerdictException(
                'This url does not seem to have valid '
                f'connection information: {url}')
        return tokenized_url[1]

    def _load_verdict_jar_into_jvm(self, gateway):
        '''
        Follows the hacky approach describe in
        https://stackoverflow.com/questions/7884393/can-a-directory-be-added-to-the-class-path-at-runtime

        Args:
            jvm: py4j's jvm instance
        '''
        # py4j's jvm interface
        jvm = gateway.jvm

        # jar URL
        verdict_jar_path = self._get_verdict_jar_path()
        verdict_jar_url = jvm.java.io.File(verdict_jar_path).toURI().toURL()

        # obtain the jvm's default class loader and cast it to URLClassLoader
        system_class_loader = jvm.java.lang.ClassLoader.getSystemClassLoader()
        url_loader_class = jvm.java.net.URLClassLoader._java_lang_class
        url_loader = url_loader_class.cast(system_class_loader)

        # get the addURL() method with reflection
        class_arr = gateway.new_array(jvm.java.lang.Class, 1)
        class_arr[0] = verdict_jar_url.getClass()
        add_url_method = url_loader_class.getDeclaredMethod("addURL", class_arr)

        # invoke the method
        add_url_method.setAccessible(True)
        url_arr = gateway.new_array(jvm.java.lang.Object, 1)
        url_arr[0] = verdict_jar_url
        add_url_method.invoke(url_loader, url_arr)

    def _get_jvm(self):
        return self._spark_context._jvm

    def _get_gateway(self, extra_class_path):
        """
        Initializes a py4j gateway.

        Args:
            class_path: Either a single str or a list of str; each str is an
                        absolute path to the jar file.
        """
        class_path = self._get_class_path(extra_class_path)
        gateway = JavaGateway.launch_gateway(
                        classpath=class_path, 
                        die_on_exit=True,
                        redirect_stdout=sys.stdout,
                        redirect_stderr=sys.stderr,
                        create_new_process_group=True)
        sleep(1)
        return gateway

    def _get_pyspark_gateway(self, spark_session):
        return spark_session._sc._gateway

    def _get_class_path(self, extra_class_path):
        """
        Returns str class path including the one for verdict jar path
        """
        lib_jar_path = self._get_lib_jars_path()
        verdict_jar_path = self._get_verdict_jar_path()

        if not os.path.isfile(verdict_jar_path):
            raise VerdictException("VerdictDB's jar file is not found.")

        str_class_path = f'{lib_jar_path}:{verdict_jar_path}'

        if extra_class_path is None:
            pass
        if isinstance(extra_class_path, str):
            str_class_path = f'{extra_class_path}:{lib_jar_path}'
        elif isinstance(extra_class_path, list):
            extra_class_path_str = ':'.join(extra_class_path)
            str_class_path = f'{extra_class_path_str}:{lib_jar_path}'

        return str_class_path

    def _get_lib_jars_path(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'lib')
        full_paths = []
        for filename in os.listdir(lib_dir):
            if filename[-3:] == 'jar':
                full_path = os.path.join(lib_dir, filename)
                full_paths.append(full_path)
        return ':'.join(full_paths)

    def _get_verdict_jar_path(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        lib_dir = os.path.join(root_dir, 'verdict_jar')
        version = self._get_verdictdb_version();
        verdict_jar_file = f'verdictdb-core-{version}-jar-with-dependencies.jar'
        verdict_jar_path = os.path.join(lib_dir, verdict_jar_file)
        return verdict_jar_path

    def _get_verdictdb_version(self):
        return verdictcommon.get_verdictdb_version()

    def _load_or_get_spark_context(self, gateway, jspark_session):
        if hasattr(self, '_context'):
            return self._context
        self._load_verdict_jar_into_jvm(gateway)
        verdict_context_class = gateway.jvm.org.verdictdb.VerdictContext
        return verdict_context_class.fromSparkSession(jspark_session)

    def _get_context(
        self,
        gateway,
        url,
        user=None,
        password=None,
        verdictdbmetaschema=None,
        verdictdbtempschema=None,
    ):
        verdict_options = self._get_verdict_options(
            gateway,
            verdictdbmetaschema,
            verdictdbtempschema,
        )

        if user is not None or password is not None:
            if user is None:
                raise ValueError('Username must be provided when a password is')
            if password is None:
                raise ValueError('Password must be provided when a username is')

            return gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(
                url,
                user,
                password,
                verdict_options,
            )

        else:
            return gateway.jvm.org.verdictdb.VerdictContext.fromConnectionString(
                url,
                verdict_options,
            )


    def _get_verdict_options(
        self,
        gateway,
        verdictdbmetaschema,
        verdictdbtempschema,
    ):
        verdict_options = gateway.jvm.org.verdictdb.commons.VerdictOption()
        verdict_options.parseProperties(
            self._get_properties(
                gateway,
                verdictdbmetaschema,
                verdictdbtempschema,
            )
        )

        return verdict_options


    def _get_properties(
        self,
        gateway,
        verdictdbmetaschema,
        verdictdbtempschema,
    ):
        properties = gateway.jvm.java.util.Properties()

        if verdictdbmetaschema is not None:
            properties.setProperty('verdictdbmetaschema', verdictdbmetaschema)
        if verdictdbtempschema is not None:
            properties.setProperty('verdictdbtempschema', verdictdbtempschema)

        return properties

