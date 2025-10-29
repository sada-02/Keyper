cd proto
buf generate
cd ..

module_name=$(awk '/^module / {print $2}' go.mod)
top_level_module_name=$(echo $module_name | cut -d'/' -f1)

if [ -d "prototypes" ]; then
    rm -rf prototypes
    fi

mv proto/$module_name/prototypes .
rm -rf proto/$top_level_module_name 
go mod tidy