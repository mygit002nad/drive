"use client"

import * as React from "react"
import { usePathname, useRouter } from "next/navigation"
import { SidebarProvider, SidebarInset, SidebarTrigger } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { Separator } from "@/components/ui/separator"
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb"
import { useAuth } from "@/components/auth-provider"
import { ThemeToggle } from "@/components/theme-toggle"

type Crumb = { label: string; href?: string }

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const pathname = usePathname()
  const router = useRouter()
  const { user, loading } = useAuth()

  const crumbs = React.useMemo<Crumb[]>(() => {
    const base: Crumb[] = [{ label: "Dashboard", href: "/dashboard/overview" }]
    if (!pathname) return [...base, { label: "Overview" }]

    if (pathname === "/dashboard" || pathname.startsWith("/dashboard/overview")) {
      return [...base, { label: "Overview" }]
    }

    if (pathname.startsWith("/dashboard/analytics")) {
      return [...base, { label: "Analytics" }]
    }

    if (pathname.startsWith("/dashboard/activity")) {
      return [...base, { label: "Activity" }]
    }

    if (pathname.startsWith("/dashboard/api-usage")) {
      return [...base, { label: "API Usage" }]
    }

    if (pathname.startsWith("/dashboard/storage")) {
      return [...base, { label: "Storage" }]
    }

    if (pathname.startsWith("/dashboard/buckets")) {
      return [...base, { label: "Buckets" }]
    }

    if (pathname.startsWith("/dashboard/projects")) {
      return [...base, { label: "Projects" }]
    }

    if (pathname.startsWith("/dashboard/accounts")) {
      return [...base, { label: "Accounts" }]
    }

    if (pathname.startsWith("/dashboard/users")) {
      return [...base, { label: "Users" }]
    }

    if (pathname.startsWith("/dashboard/settings")) {
      return [...base, { label: "Settings" }]
    }

    if (pathname.startsWith("/dashboard/migrations")) {
      const parts = pathname.split("/").filter(Boolean)
      const sub = parts[2] ?? ""

      if (!sub) {
        return [...base, { label: "Migrations" }]
      }

      if (sub === "history") {
        return [...base, { label: "Migrations", href: "/dashboard/migrations" }, { label: "History" }]
      }

      if (parts[3] === "worker-pool") {
        return [
          ...base,
          { label: "Migrations", href: "/dashboard/migrations" },
          { label: "Migration", href: `/dashboard/migrations/${encodeURIComponent(sub)}` },
          { label: "Worker pool" },
        ]
      }

      // /dashboard/migrations/[id]
      return [...base, { label: "Migrations", href: "/dashboard/migrations" }, { label: sub }]
    }

    if (pathname.startsWith("/dashboard/agents") || pathname.startsWith("/dashboard/workers")) {
      return [...base, { label: "Workers" }]
    }

    return [...base, { label: "Dashboard" }]
  }, [pathname])

  React.useEffect(() => {
    if (loading) return
    if (!user || user.role === "user") {
      router.replace("/?forbidden=dashboard")
    }
  }, [user, loading, router])

  if (loading || !user || user.role === "user") {
    return null
  }

  return (
    <SidebarProvider className="h-svh min-h-0 overflow-hidden overscroll-none">
      <AppSidebar />
      <SidebarInset className="h-full min-h-0 min-w-0 touch-pan-y overflow-x-hidden overflow-y-auto overscroll-y-contain [-webkit-overflow-scrolling:touch]">
        <header className="dashboard-header-glass sticky top-0 z-50 overflow-hidden border-b transition-[background-color,backdrop-filter,border-color]">
          <div className="page-shell relative z-10 flex min-h-14 items-center justify-between gap-2 px-4 sm:h-16 sm:px-6">
            <div className="flex min-w-0 flex-1 items-center gap-2">
              <SidebarTrigger className="-ml-1 !grid place-items-center !border-0 !p-0 !shadow-none [&>svg]:!m-0 [&>svg]:!size-4 [&>svg]:place-self-center" />
              <Separator orientation="vertical" className="mr-1 !h-3 bg-foreground/20 sm:mr-2" />
              <Breadcrumb className="min-w-0">
                <BreadcrumbList className="min-w-0 flex-nowrap overflow-hidden">
                  {crumbs.map((c, index) => {
                    const isLast = index === crumbs.length - 1
                    return (
                      <React.Fragment key={`${c.label}-${index}`}>
                        {index > 0 ? <BreadcrumbSeparator className="hidden md:block" /> : null}
                        <BreadcrumbItem className={index === 0 ? "hidden md:block" : "min-w-0"}>
                          {isLast || !c.href ? (
                            <BreadcrumbPage className="truncate">{c.label}</BreadcrumbPage>
                          ) : (
                            <BreadcrumbLink href={c.href} className="truncate">
                              {c.label}
                            </BreadcrumbLink>
                          )}
                        </BreadcrumbItem>
                      </React.Fragment>
                    )
                  })}
                </BreadcrumbList>
              </Breadcrumb>
            </div>
            <div className="flex shrink-0 items-center gap-1">
              <ThemeToggle
                className="h-9 w-9 rounded-full border border-[var(--border)] bg-transparent p-0 text-[var(--foreground)] hover:bg-transparent"
                iconClassName="h-[18px] w-[18px] stroke-[2.25]"
              />
            </div>
          </div>
        </header>
        <div className="-mt-16 flex min-w-0 flex-1 flex-col gap-4 overflow-x-hidden px-3 pt-20 pb-4 sm:px-4 lg:px-6">
          {children}
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
