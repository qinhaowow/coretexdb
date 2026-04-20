from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="ai-robot-memory-system",
    version="0.1.0",
    author="AI Robot Team",
    author_email="team@example.com",
    description="AI Robot Memory System with CortexDB, RAG, ROS2 Integration, and LLM Decision Making",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/example/ai-robot-memory-system",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "ros2": [
            "rclpy>=3.3.0",
            "sensor-msgs>=4.2.0",
            "nav-msgs>=4.2.0",
            "geometry-msgs>=4.2.0",
            "std-msgs>=4.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ai-memory-node=ros2_integration.memory_node:main",
            "ai-memory-query=ros2_integration.query_service:main",
            "ai-benchmark=scripts.benchmark:main",
            "ai-migrate=scripts.migrate_data:main",
        ],
    },
    include_package_data=True,
    package_data={
        "ai_robot_memory_system": ["config/*.yaml"],
    },
    zip_safe=False,
)
