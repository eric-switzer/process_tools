from distutils.core import setup

setup(
    name='process_tools',
    version='0.1dev',
    description='Python parallel/persistent process tools',
    author='Eric Switzer',
    packages=['process_tools',],
    license='GPL',
    long_description=open('README.md').read(),
    scripts = [
        'scripts/run_process_daemon.py'
    ]
)
