package com.aiforge.collab

import android.app.Application
import com.aiforge.collab.store.WorkspaceRepo

class App : Application() {

    lateinit var repo: WorkspaceRepo
        private set

    override fun onCreate() {
        super.onCreate()
        repo = WorkspaceRepo(this)
    }
}
