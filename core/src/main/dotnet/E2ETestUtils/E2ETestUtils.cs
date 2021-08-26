// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Linq;
using Xunit.Sdk;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Services;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Xunit;

namespace MMLSparktest.Utils
{
    /// <summary>
    /// Creates a temporary folder that is automatically cleaned up when disposed.
    /// </summary>
    public class TemporaryDirectory : IDisposable
    {
        private bool _disposed = false;

        /// <summary>
        /// Path to temporary folder.
        /// </summary>
        public string Path { get; }

        public TemporaryDirectory()
        {
            Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString());
            Cleanup();
            Directory.CreateDirectory(Path);
            Path = $"{Path}{System.IO.Path.DirectorySeparatorChar}";
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Cleanup()
        {
            if (File.Exists(Path))
            {
                File.Delete(Path);
            }
            else if (Directory.Exists(Path))
            {
                Directory.Delete(Path, true);
            }
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                Cleanup();
            }

            _disposed = true;
        }
    }

    internal static class SparkSettings
    {
        internal static Version Version { get; private set; }
        internal static string SparkHome { get; private set; }

        static SparkSettings()
        {
            InitSparkHome();
            InitVersion();
        }

        private static void InitSparkHome()
        {
            SparkHome = Environment.GetEnvironmentVariable("SPARK_HOME");
            if (SparkHome == null)
            {
                throw new NullException("SPARK_HOME environment variable is not set.");
            }
        }

        private static void InitVersion()
        {
            // First line of the RELEASE file under SPARK_HOME will be something similar to:
            // Spark 2.4.0 built for Hadoop 2.7.3
            string firstLine =
                File.ReadLines($"{SparkHome}{Path.DirectorySeparatorChar}RELEASE").First();

            // Grab "2.4.0" from "Spark 2.4.0 built for Hadoop 2.7.3"
            string versionStr = firstLine.Split(' ')[1];

            // Strip anything below version number.
            // For example, "3.0.0-preview" should become "3.0.0".
            Version = new Version(versionStr.Split('-')[0]);
        }
    }

     internal static class TestEnvironment
    {
        private static string s_resourceDirectory;
        internal static string ResourceDirectory
        {
            get
            {
                if (s_resourceDirectory is null)
                {
                    s_resourceDirectory =
                        AppDomain.CurrentDomain.BaseDirectory +
                        Path.DirectorySeparatorChar +
                        "Resources" +
                        Path.DirectorySeparatorChar;
                }

                return s_resourceDirectory;
            }
        }
    }

    /// <summary>
    /// SparkFixture acts as a global fixture to start Spark application in a debug
    /// mode through the spark-submit. It also provides a default SparkSession
    /// object that any tests can use.
    /// </summary>
    public sealed class SparkFixture : IDisposable
    {
        /// <summary>
        /// The names of environment variables used by the SparkFixture.
        /// </summary>
        public class EnvironmentVariableNames
        {
            /// <summary>
            /// This environment variable specifies extra args passed to spark-submit.
            /// </summary>
            public const string ExtraSparkSubmitArgs =
                "DOTNET_SPARKFIXTURE_EXTRA_SPARK_SUBMIT_ARGS";

            /// <summary>
            /// This environment variable specifies the path where the DotNet worker is installed.
            /// </summary>
            public const string WorkerDir = ConfigurationService.DefaultWorkerDirEnvVarName;
        }

        private readonly Process _process = new Process();
        private readonly TemporaryDirectory _tempDirectory = new TemporaryDirectory();

        public const string DefaultLogLevel = "ERROR";

        public SparkSession Spark { get; }

        public IJvmBridge Jvm { get; }

        public SparkFixture()
        {
            // The worker directory must be set for the Microsoft.Spark.Worker executable.
            if (string.IsNullOrEmpty(
                Environment.GetEnvironmentVariable(EnvironmentVariableNames.WorkerDir)))
            {
                throw new Exception(
                    $"Environment variable '{EnvironmentVariableNames.WorkerDir}' must be set.");
            }

            BuildSparkCmd(out var filename, out var args);

            // Configure the process using the StartInfo properties.
            _process.StartInfo.FileName = filename;
            _process.StartInfo.Arguments = args;
            // UseShellExecute defaults to true in .NET Framework,
            // but defaults to false in .NET Core. To support both, set it
            // to false which is required for stream redirection.
            _process.StartInfo.UseShellExecute = false;
            _process.StartInfo.RedirectStandardInput = true;
            _process.StartInfo.RedirectStandardOutput = true;
            // _process.StartInfo.RedirectStandardError = true;

            bool isSparkReady = false;
            _process.OutputDataReceived += (sender, arguments) =>
            {
                // Scala-side driver for .NET emits the following message after it is
                // launched and ready to accept connections.
                if (!isSparkReady &&
                    arguments.Data.Contains("Backend running debug mode"))
                {
                    isSparkReady = true;
                }
            };

            _process.Start();
            _process.BeginOutputReadLine();
            // _process.BeginErrorReadLine();

            bool processExited = false;
            while (!isSparkReady && !processExited)
            {
                processExited = _process.WaitForExit(500);
            }

            if (processExited)
            {
                _process.Dispose();

                // The process should not have been exited.
                throw new Exception(
                    $"Process exited prematurely with '{filename} {args}'.");
            }

            Spark = SparkSession
                .Builder()
                // Lower the shuffle partitions to speed up groupBy() operations.
                .Config("spark.sql.shuffle.partitions", "3")
                .Config("spark.ui.enabled", true)
                .Config("spark.ui.showConsoleProgress", true)
                .AppName("SelectColumns Test")
                .GetOrCreate();

            Spark.SparkContext.SetLogLevel(DefaultLogLevel);

            Jvm = Spark.Reference.Jvm;
        }

        public string AddPackages(string args)
        {
            string packagesOption = "--packages ";
            string[] splits = args.Split(packagesOption, 2);

            StringBuilder newArgs = new StringBuilder(splits[0])
                .Append(packagesOption)
                .Append(GetAvroPackage())
                .Append(",")
                .Append(GetMMLSparkPackage());
            if (splits.Length > 1)
            {
                newArgs.Append(",").Append(splits[1]);
            }

            return newArgs.ToString();
        }

        public string GetAvroPackage()
        {
            Version sparkVersion = SparkSettings.Version;
            string avroVersion = sparkVersion.Major switch
            {
                2 => $"spark-avro_2.11:{sparkVersion}",
                3 => $"spark-avro_2.12:{sparkVersion}",
                _ => throw new NotSupportedException($"Spark {sparkVersion} not supported.")
            };

            return $"org.apache.spark:{avroVersion}";
        }

        public string GetMMLSparkPackage()
        {
            return "com.microsoft.ml.spark:mmlspark:1.0.0-rc3-205-c837431c-20210826-1724-SNAPSHOT";
        }

        public string AddMMLSparkRepo()
        {
            bool addMMLSparkRepo = true;
            if (addMMLSparkRepo)
            {
                return "--repositories  https://mmlspark.blob.core.windows.net/maven/";
            }
            return "";
        }

        public string AddLocalMMLSparkJar()
        {
            bool addLocalMMLSparkJar = true;
            if (addLocalMMLSparkJar)
            {
                return "--jars D:\\repos\\mmlspark\\target\\scala-2.12\\mmlspark-1.0.0-rc3-148-87ec5f74-SNAPSHOT.jar";
            }
            return "";
        }

        public void Dispose()
        {
            Spark.Dispose();

            // CSparkRunner will exit upon receiving newline from
            // the standard input stream.
            _process.StandardInput.WriteLine("done");
            _process.StandardInput.Flush();
            _process.WaitForExit();

            _tempDirectory.Dispose();
        }

        private void BuildSparkCmd(out string filename, out string args)
        {
            string sparkHome = SparkSettings.SparkHome;

            // Build the executable name.
            filename = Path.Combine(sparkHome, "bin", "spark-submit");
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                filename += ".cmd";
            }

            if (!File.Exists(filename))
            {
                throw new FileNotFoundException($"{filename} does not exist.");
            }

            // Build the arguments for the spark-submit.
            string classArg = "--class org.apache.spark.deploy.dotnet.DotnetRunner";
            string curDir = AppDomain.CurrentDomain.BaseDirectory;
            string jarPrefix = GetJarPrefix();
            // string scalaDir = Path.Combine(curDir, "..", "..", "..", "..", "..", "src", "scala");
            // string jarDir = Path.Combine(curDir, jarPrefix, "target");
            string jarDir = curDir;
            // string assemblyVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString(3);
            string assemblyVersion = "2.0.0";
            string scalaVersion = (SparkSettings.Version.Major == 3) ? "2.12" : "2.11";
            string jar = Path.Combine(jarDir, $"{jarPrefix}_{scalaVersion}-{assemblyVersion}.jar");

            if (!File.Exists(jar))
            {
                throw new FileNotFoundException($"{jar} does not exist.");
            }

            string warehouseUri = new Uri(
                Path.Combine(_tempDirectory.Path, "spark-warehouse")).AbsoluteUri;
            string warehouseDir = $"--conf spark.sql.warehouse.dir={warehouseUri}";

            string extraArgs = Environment.GetEnvironmentVariable(
                EnvironmentVariableNames.ExtraSparkSubmitArgs) ?? "";

            // If there exists log4j.properties in SPARK_HOME/conf directory, Spark from 2.3.*
            // to 2.4.0 hang in E2E test. The reverse behavior is true for Spark 2.4.1; if
            // there does not exist log4j.properties, the tests hang.
            // Note that the hang happens in JVM when it tries to append a console logger (log4j).
            // The solution is to use custom log configuration that appends NullLogger, which
            // works across all Spark versions.
            string resourceUri = new Uri(TestEnvironment.ResourceDirectory).AbsoluteUri;
            string logOption = "--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=" +
                $"{resourceUri}/log4j.properties";

            args = $"{logOption} {warehouseDir} {AddPackages(extraArgs)} {AddMMLSparkRepo()} {classArg} --master local {jar} debug";
        }

        private string GetJarPrefix()
        {
            Version sparkVersion = SparkSettings.Version;
            return $"microsoft-spark-{sparkVersion.Major}-{sparkVersion.Minor}";
        }
    }

    [CollectionDefinition("Spark E2E Tests")]
    public class SparkCollection : ICollectionFixture<SparkFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }

    public class FeatureBaseTests<T>
    {
        private readonly SparkSession _spark;

        protected FeatureBaseTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// Tests the common functionality across all ML.Feature classes.
        /// </summary>
        /// <param name="testObject">The object that implemented FeatureBase</param>
        /// <param name="paramName">The name of a parameter that can be set on this object</param>
        /// <param name="paramValue">A parameter value that can be set on this object</param>
        public void TestFeatureBase(
            FeatureBase<T> testObject,
            string paramName,
            object paramValue)
        {
            Assert.NotEmpty(testObject.ExplainParams());

            Param param = testObject.GetParam(paramName);
            Assert.NotEmpty(param.Doc);
            Assert.NotEmpty(param.Name);
            Assert.Equal(param.Parent, testObject.Uid());

            Assert.NotEmpty(testObject.ExplainParam(param));
            testObject.Set(param, paramValue);
            Assert.IsAssignableFrom<Identifiable>(testObject.Clear(param));

            Assert.IsType<string>(testObject.Uid());
        }
    }

}
