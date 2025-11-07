# WQ

A small tool that allows you to execute and preview CQL queries directly from Zed editor.

<img width="2299" height="1006" alt="image" src="https://github.com/user-attachments/assets/7f16df48-4d35-4278-b0b0-5209355e1828" />

<img width="2526" height="1022" alt="image" src="https://github.com/user-attachments/assets/d9c937ab-7307-408a-8d89-f99b4ecad760" />


### Installation 

```sh
cargo install wq-zed
```

### Keybindings

Example keybinds for zed

```json
{
    "context": "Editor && vim_mode == visual && !menu",
    "bindings": {
      "[ c": ["task::Spawn", { "task_name": "Execute query" }]
    }
}
```
> [!NOTE]
> You don't need to create `Execute query` task, as it's provided by [extension](https://github.com/Akzestia/zed-cql)

> [!IMPORTANT]
> This tool uses the same env variables as [cqlls](https://github.com/Akzestia/cqlls). </br>
> Please check the language server docs before using `WQ`

