"""
Setup configuration for WoW Raid Telemetry Pipeline.
Instala el paquete en modo desarrollo (editable).
"""

from setuptools import setup, find_packages

setup(
    name="wow-telemetry-pipeline",
    version="0.1.0",
    description="Event-driven telemetry pipeline for WoW raids with Medallion architecture",
    author="Pipeline WoW Team",
    author_email="tu-email@ejemplo.com",
    
    # Descubrimiento automático de paquetes en src/
    packages=find_packages(where="."),
    package_dir={"": "."},
    
    # Dependencias de producción
    install_requires=[
        "pydantic>=2.10.4",
        "numpy>=1.26.4",
    ],
    
    # Dependencias de desarrollo
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-cov>=4.1.0",
            "black>=24.1.0",
            "ruff>=0.1.15",
            "mypy>=1.8.0",
        ]
    },
    
    # Versión mínima de Python
    python_requires=">=3.10",
    
    # Clasificadores PyPI
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
