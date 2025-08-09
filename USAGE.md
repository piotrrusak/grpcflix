# Step 1:

## Termianal 1:

```bash
./script/setup.sh
```

# Step 2: (step 1 must end before starting step 2)
Copy your .mp4 file to streamer/resource/.

# Step 3: (step 2 must end before starting step 3)

## Terminal 1 & 2: (best in vscode split teminal)

```bash
cd client/
source .venv/bin/activate
python3 main.py
```

## Terminal 3: (best in vscode split teminal)

```bash
cd server/
source .venv/bin/activate
python3 main.py
```

## Terminal 4: (best external system terminal)

### Substep 1:
```bash
cd streamer/
source .venv/bin/activate
python3 main.py
```

### Substep 2:
```bash
sao1.mp4 # For development i use file named: "sao1.mp4", you enter name of your own .mp4 file.
```