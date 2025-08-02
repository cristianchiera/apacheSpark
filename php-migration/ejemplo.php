<?php
// Ejemplo de archivo PHP - Reemplaza con tu código real
class UserController {
    public function index() {
        $users = User::all();
        return response()->json($users);
    }
    
    public function store(Request $request) {
        $user = User::create($request->all());
        return response()->json($user, 201);
    }
}