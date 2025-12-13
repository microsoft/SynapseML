from pyspark.sql import SparkSession


def check_access(spark, package_name, class_name, member_name, is_method=False):
    """
    Checks if a private member of a class is accessible via reflection.
    """
    try:
        jvm = spark.sparkContext._gateway.jvm

        # 1. Get the Class
        clazz = jvm.java.lang.Class.forName(class_name)

        # 2. Try to get the declared field/method (which might be private)
        try:
            if is_method:
                # Assuming 0 args for simplicity of generic test, or we'd need signature
                # This is a bit heuristic for generic checks
                member = clazz.getDeclaredMethod(member_name, None)
            else:
                member = clazz.getDeclaredField(member_name)
        except Exception as e:
            # If we can't even find it, that's a different issue (maybe it doesn't exist in this JDK)
            # But usually getDeclaredField works, it's setAccessible that fails
            return f"SKIP: {class_name}.{member_name} not found or signature mismatch: {str(e)}"

        # 3. Try to set Accessible
        try:
            member.setAccessible(True)
            return f"SUCCESS: {package_name} is accessible."
        except Exception as e:
            if "InaccessibleObjectException" in str(e):
                return f"FAIL: {package_name} is BLOCKED. ({str(e)})"
            return f"ERROR: Unexpected error checking {package_name}: {str(e)}"

    except Exception as e:
        return f"CRITICAL: Could not attempt check for {package_name}: {str(e)}"


def verify_fabric_environment():
    spark = SparkSession.builder.appName("FabricAccessCheck").getOrCreate()

    print("--- Verifying Java Module Access ---")

    # List of checks derived from SynapseML build.sbt
    # Format: (Package, Class, Private Member)
    # We choose likely private/protected members that would require 'opens'

    checks = [
        ("java.sql", "java.sql.Date", "fastTime"),  # The specific culprit
        ("java.util.prefs", "java.util.prefs.AbstractPreferences", "newNode"),
        ("java.lang", "java.lang.ClassLoader", "classes"),
        ("java.io", "java.io.ObjectOutputStream", "enableReplaceObject"),
        ("java.net", "java.net.URLClassLoader", "acc"),
        ("java.nio", "java.nio.Buffer", "address"),
        ("java.util", "java.util.ArrayList", "elementData"),
        ("sun.nio.ch", "sun.nio.ch.SelectorImpl", "selectedKeys"),
        # Add more as needed based on the 'opens' list
    ]

    results = []

    for pkg, cls, member in checks:
        result = check_access(spark, pkg, cls, member)
        results.append(result)
        print(result)

    print("\n--- Summary ---")
    failures = [r for r in results if "FAIL" in r]
    if failures:
        print(f"FOUND {len(failures)} FAILURES:")
        for f in failures:
            print(f)
    else:
        print(
            "ALL CHECKS PASSED. This environment behaves like it has the correct --add-opens flags."
        )

    spark.stop()


if __name__ == "__main__":
    verify_fabric_environment()
