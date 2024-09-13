import os

def save_directory_structure(root_dir, output_file):
    with open(output_file, 'w') as f:
        for root, dirs, files in os.walk(root_dir):
            level = root.replace(root_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            f.write(f'{indent}{os.path.basename(root)}/\n')
            sub_indent = ' ' * 4 * (level + 1)
            for file in files:
                f.write(f'{sub_indent}{file}\n')

# Usage:
root_dir = r'C:\Users\diogo\Desktop\ML\1-Tools\0-Python scripts\chess_project2'
output_file = 'folder_structure.txt'
save_directory_structure(root_dir, output_file)
