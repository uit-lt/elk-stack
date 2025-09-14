#!/usr/bin/env python3
"""
Auto-generate Jupyter notebooks from Spark Python files
This script will create notebooks based on the current Python files in ../spark/
"""
import json
import os
from pathlib import Path

def create_notebook_from_template(title, cells):
    """Create a Jupyter notebook structure"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"# {title}"]
            }
        ] + cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def python_to_cells(python_content):
    """Convert Python content to notebook cells"""
    lines = python_content.split('\n')
    cells = []
    current_cell = []

    i = 0
    while i < len(lines):
        line = lines[i]

        # Group imports and function definitions
        if (line.startswith('from ') or line.startswith('import ') or
            line.startswith('def ') or line.strip() == '' or line.startswith('    ')):
            current_cell.append(line)

        # New cell for main function content
        elif 'print("===' in line:
            if current_cell and any(l.strip() for l in current_cell):
                cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [l for l in current_cell if l.strip()]
                })
                current_cell = []
            current_cell.append(line)

        # Skip main() call and if __name__ check
        elif line.strip() in ['main()', 'if __name__ == "__main__":']:
            pass

        # Regular code lines
        else:
            current_cell.append(line)

            # Create new cell on logical breaks
            if (i + 1 < len(lines) and
                (lines[i + 1].strip().startswith('#') or
                 'print(' in lines[i + 1] or
                 lines[i + 1].strip() == '')):
                if current_cell and any(l.strip() for l in current_cell):
                    cells.append({
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": [l for l in current_cell if l.strip()]
                    })
                    current_cell = []

        i += 1

    # Add remaining content
    if current_cell and any(l.strip() for l in current_cell):
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [l for l in current_cell if l.strip()]
        })

    return cells

def main():
    """Generate notebooks from Python files"""
    print("🚀 Auto-generating Spark Elasticsearch notebooks...")

    # File mappings
    files_to_convert = [
        ("01_get_all_data.py", "01_Get_All_Data.ipynb", "📊 Get All Data from Elasticsearch"),
        ("02_insert_5_records.py", "02_Insert_5_Records.ipynb", "📝 Insert 5 Records"),
        ("03_update_5_records.py", "03_Update_5_Records.ipynb", "🔄 Update 5 Records"),
        ("04_delete_5_records.py", "04_Delete_5_Records.ipynb", "🗑️ Delete 5 Records"),
        ("05_query_by_ids.py", "05_Query_By_IDs.ipynb", "🔍 Query Records by IDs")
    ]

    current_dir = Path(__file__).parent
    spark_dir = current_dir.parent / "spark"

    print("=" * 50)

    created_count = 0
    for py_file, notebook_file, title in files_to_convert:
        py_path = spark_dir / py_file
        notebook_path = current_dir / notebook_file

        if py_path.exists():
            try:
                # Read Python file
                with open(py_path, 'r', encoding='utf-8') as f:
                    python_content = f.read()

                # Convert to cells
                cells = python_to_cells(python_content)

                # Create notebook
                notebook = create_notebook_from_template(title, cells)

                # Write notebook
                with open(notebook_path, 'w', encoding='utf-8') as f:
                    json.dump(notebook, f, indent=2, ensure_ascii=False)

                print(f"✓ Created: {notebook_file}")
                created_count += 1

            except Exception as e:
                print(f"✗ Error creating {notebook_file}: {e}")
        else:
            print(f"⚠️  Python file not found: {py_path}")

    print("=" * 50)
    print(f"🎉 Created {created_count} notebooks successfully!")

    if created_count > 0:
        print("\nYou can now run these notebooks in Jupyter Lab:")
        for _, notebook_file, _ in files_to_convert[:created_count]:
            print(f"  - {notebook_file}")

if __name__ == "__main__":
    main()