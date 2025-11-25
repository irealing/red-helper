from setuptools import setup, find_packages

__author__ = 'Memory_Leak<irealing@163.com>'
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()
setup(
    name='red_helper',
    packages=find_packages(),
    author="Memory_Leak",
    version="0.1.2",
    auth_email="irealing@163.com",
    python_requires=">=3.9",
    description="基于异步IO的Redis缓存操作工具",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/irealing/red-helper",
    platforms='any',
    license="MIT"
)
